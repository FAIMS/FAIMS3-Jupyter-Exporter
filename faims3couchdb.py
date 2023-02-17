import sys
import logging
import requests
import re
import tqdm
from uuid import uuid4
import base64
import traceback
from slugify import slugify
from collections import OrderedDict, defaultdict
import datetime
import geojson
import json
from shapely.geometry import shape
from pprint import pprint

# from flatten_json import flatten

from pprint import pformat
import logging
import tempfile
import pandas

from mimetypes import guess_extension, guess_type

LOCAL_TIMEZONE = datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo


class TqdmLoggingHandler(logging.Handler):
    # https://stackoverflow.com/a/38739634
    def __init__(self, level=logging.NOTSET):
        super().__init__(level)

    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.tqdm.write(msg)
            self.flush()
        except Exception:
            self.handleError(record)


log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
log.addHandler(TqdmLoggingHandler())


def create_new_revision(
    *, avps, record_id, new_id, parents, created_by, created, type, deleted=False
):
    """
    Create a new revision (format v1).

    avps must be a dictionary mapping field names to avp id
    parents must be a list of revision ids
    """
    return {
        "_id": new_id,
        "revision_format_version": 1,
        "avps": avps,
        "record_id": record_id,
        "parents": parents,
        "created": str(created),
        "created_by": created_by,
        "type": type,
        "deleted": deleted,
    }


def create_new_avp(
    *, data, revision_id, record_id, annotations, type, attachments=None
):
    """
    Create a new attribute-value pair (format v1).

    data can be anything that can be json encoded
    """
    avp = {
        "_id": f"avp-{uuid4()}",
        "avp_format_version": 1,
        "type": type,
        "record_id": record_id,
        "revision_id": revision_id,
        "data": data,
        "annotations": annotations,
    }
    if attachments:
        avp["_attachments"] = attachments
    return avp


class BearerAuth(requests.auth.AuthBase):
    # https://stackoverflow.com/a/58055668
    def __init__(self, token):
        self.token = token

    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token
        return r


class CouchDBHelper:
    def __init__(
        self,
        *,
        user,
        token,
        base_url,
        project_key,
        include_deleted=False,
        for_export=True,
        bearer_token=None,
    ):
        self.user = user
        self.token = token
        self.bearer_token = bearer_token
        self.auth_token = None
        self.base_url = base_url
        self.records = []
        self.project = None
        self.metadata = None
        self.project_id = None
        self.project_metadata = {}
        self.record_type_names = {}
        self.record_definitions = None
        self.record_fieldnames = defaultdict(list)
        self.record_fieldnames_odict = defaultdict(dict)
        self.include_deleted = include_deleted
        self.identifiers = {}
        self.forms_from_record_id = {}
        project_url = f"{self.base_url}/projects/{project_key}"
        """
        Initialise by getting project data, and project metadata keys and project id
        """
        if user:
            self.auth_token = (user, token)
        if bearer_token:
            self.auth_token = BearerAuth(bearer_token)

        # logging.debug(f"Initialising with {project_url}")
        r = requests.get(project_url, auth=self.auth_token)
        r.raise_for_status()

        project_data = r.json()

        # logging.debug(f"Seen: {project_data}")
        assert project_data.get(
            "name"
        ), f"Project: {self.project_key} not found on server: {self.base_url}. Aborting."
        assert project_data.get(
            "metadata_db"
        ), f"Project: {self.project_key} not found on server: {self.base_url}. Aborting."
        assert project_data.get(
            "data_db"
        ), f"Project: {self.project_key} not found on server: {self.base_url}. Aborting."

        self.project_id = project_data["name"]

        self.project = project_data["data_db"]["db_name"]
        self.metadata = project_data["metadata_db"]["db_name"]
        self.project_metadata_attachments = {}
        self.multivalued_fields = self.get_multivalued_fields()
        self.record_count = defaultdict(int)
        self.fetch_field_metadata()
        # if for_export:
        #     self.fetch_and_flatten_records()
        self.fetch_project_metadata()

    def make_request_get(self, url):
        # print(self.user, self.token, self.bearer_token)
        # if self.user and self.token:
        #     print("Using user auth")
        #     r = requests.get(url, auth=(self.user, self.token))
        #     r.raise_for_status()
        #     return r
        # if self.bearer_token:
        #     print("using bearer token")
        #     r = requests.get(url, headers= {"Authorization": f"Bearer {self.bearer_token}"})
        #     r.raise_for_status()
        #     return r
        r = requests.get(url, auth=self.auth_token)
        r.raise_for_status()
        return r
        raise ValueError("Unable to authenticate with credentials provided")

    def get_multivalued_fields(self):
        """
        Get field names of fields which support multiple stored values.

        Right now it only supports multi-select-fields. Returns a dict of
        fields and their possible values.
        """
        multivalued_fields = {}
        url = f"{self.base_url}/{self.metadata}/ui-specification"

        # r = requests.get(url, auth=(self.user, self.token))
        # r.raise_for_status()
        r = self.make_request_get(url)

        for element in r.json()["fields"]:
            data = r.json()["fields"][element]
            if data["component-parameters"].get("SelectProps", {}).get("multiple"):
                # print("multi", data)

                multivalued_fields[element] = {
                    "name": element,
                    "values": data["component-parameters"]
                    .get("ElementProps", {})
                    .get("options", []),
                }
        return multivalued_fields

    def fetch_field_metadata(self):
        """
        Fetch field metadata and set field_mapping, field_types, field_metadata

        The human_dict_name_map allows easy mapping of field-names to human-displayed InputLabelProps.
        The field_types maps field-names to their type-returned
        And field_metadata is the entire field metadata for processing elsewhere.
        """

        human_dict_name_map = {}
        record_type_names = {}
        field_types = {}
        field_metadata = {}
        views = {}
        record_types = defaultdict(OrderedDict)
        element_hierarchy = defaultdict(defaultdict)
        dupe_check = defaultdict(list)
        url = f"{self.base_url}/{self.metadata}/ui-specification"

        # r = requests.get(url, auth=(self.user, self.token))
        # r.raise_for_status()
        r = self.make_request_get(url)

        for record in r.json()["viewsets"]:
            label = r.json()["viewsets"][record]["label"]
            record_type_names[record] = label
            logging.debug(f"{record=}{label=}")

        for element in r.json()["fields"]:
            data = r.json()["fields"][element]
            human_element = (
                data["component-parameters"]
                .get("InputLabelProps", {})
                .get("label", None)
                or data["component-parameters"]
                .get("FormControlLabelProps", {})
                .get("label", None)
                or data["component-parameters"].get("label", None)
                or element
            )
            # print(element, human_element)
            # pprint(data)
            field_types[element] = data["type-returned"]
            dupe_check[human_element].append(element)
            human_dict_name_map[element] = human_element
            field_metadata[element] = data
            if data.get("meta", {}).get("annotation"):
                anno_element = f"{element} annotation"
                anno_human_element = (
                    f"{human_element} ({data['meta']['annotation_label']})"
                )
                field_types[anno_element] = "faims-core::String"
                dupe_check[anno_human_element].append(anno_element)
                human_dict_name_map[anno_element] = anno_human_element
                field_metadata[anno_element] = data

            if data.get("meta", {}).get("uncertainty").get("include"):
                anno_element = f"{element} uncertainty"
                anno_human_element = (
                    f"{human_element} ({data['meta']['uncertainty']['label']})"
                )
                # print("uncertain!", anno_element, anno_human_element)
                field_types[anno_element] = "faims-core::Boolean"
                dupe_check[anno_human_element].append(anno_element)
                human_dict_name_map[anno_element] = anno_human_element
                field_metadata[anno_element] = data

        for human_element in dupe_check:
            if len(dupe_check[human_element]) > 1:
                # print("dupe", human_element)
                for element in dupe_check[human_element]:
                    human_dict_name_map[element] = f"{human_element} ({element})"

        # for view in r.json()['fviews']:
        #     #print(f"{view=}")
        #     views[view] = r.json()['fviews'][view]
        # #pprint(views)
        # for viewset in r.json()['viewsets']:
        #     #print(f"{viewset=}")
        #     for view in r.json()['viewsets'][viewset]['views']:
        #         #print(f"{view=}")
        #         for element in views[view]['fields']:
        #             #print(f"{element=}")
        #             record_types[viewset][element] = field_metadata[element]
        #             element_hierarchy[element] = {"viewset":viewset,
        #                                           "view":view,
        #                                           "element": field_metadata[element]}
        # pprint(element_hierarchy, width=150)
        self.record_definitions = record_types
        self.field_mapping = human_dict_name_map
        self.field_types = field_types
        self.field_metadata = field_metadata
        self.element_hierarchy = element_hierarchy
        self.record_type_names = record_type_names

    def get_records(self):
        """
        Get all records for a particular project.

        Returns a list of dictionaries, with each dictionary containing the
        details of a specific record. Specifics of the dictionary come from the
        ``EncodedRecord`` interface in the datamodel, but of most interest will
        be ``created`` and ``created_by`` to find out who created the record
        initially.
        """
        url = f"{self.base_url}/{self.project}/_find"
        limit = 25
        result = []

        def page_results(url, bookmark=None, limit=25):
            r = requests.post(
                url,
                auth=self.auth_token,
                json={
                    "selector": {
                        "record_format_version": 1,
                    },
                    "bookmark": bookmark,
                    # we're going to get everything, we could do filtering as per
                    # https://docs.couchdb.org/en/stable/api/database/find.html
                },
            )
            r.raise_for_status()

            # print("get_records")
            # pprint(r.json())
            return r.json()

        req = page_results(url, None, limit)
        current_docs = req["docs"]
        bookmark = req["bookmark"]
        result = result + current_docs

        while len(current_docs) >= limit:
            # Note that the presence of a bookmark doesn’t guarantee that there are more results. You can to test whether you have reached the end of the result set by comparing the number of results returned with the page size requested - if results returned < limit, there are no more.
            # https://docs.couchdb.org/en/stable/api/database/find.html#pagination
            req = page_results(url, bookmark, limit)
            current_docs = req["docs"]
            bookmark = req["bookmark"]
            result = result + current_docs
            # print(bookmark, len(current_docs))
        return result

    def get_head_revisions_for_record(self, record):
        """
        Get all head revisions for a particular record.

        Returns a dictionary of dictionaries, with the key being the revision
        id, and the value dictionary containing the details of a specific
        revision. Specifics of the dictionary come from the ``Revision``
        interface in the datamodel.
        """
        url = f"{self.base_url}/{self.project}/_all_docs"
        r = requests.post(
            url,
            auth=self.auth_token,
            json={
                "keys": record["heads"],
                "include_docs": True,
            },
        )
        r.raise_for_status()

        # print("head revs")
        # pprint({row["doc"]["_id"]: row["doc"] for row in r.json()["rows"]})

        try:
            return {row["doc"]["_id"]: row["doc"] for row in r.json()["rows"]}
        except:
            return None

    def get_all_revisions_for_record(self, record):
        """
        Get all revisions for a particular record.

        Returns a dictionary of dictionaries, with the key being the revision
        id, and the value dictionary containing the details of a specific
        revision. Specifics of the dictionary come from the ``Revision``
        interface in the datamodel.
        """
        url = f"{self.base_url}/{self.project}/_all_docs"
        r = requests.post(
            url,
            auth=self.auth_token,
            json={
                "keys": record["revisions"],
                "include_docs": True,
            },
        )
        r.raise_for_status()
        return {row["doc"]["_id"]: row["doc"] for row in r.json()["rows"]}

    def get_all_avps_for_revision(self, revision):
        """
        Get all attribute value pairs (avps) for a particular revision.

        Returns a dictionary of dictionaries, with the key being the avp
        id, and the value dictionary containing the details of a specific
        avp. Specifics of the dictionary come from the ``AttributeValuePair``
        interface in the datamodel.
        """

        # print(revision)
        url = f"{self.base_url}/{self.project}/_all_docs"
        r = requests.post(
            url,
            auth=self.auth_token,
            json={
                "keys": list(revision["avps"].values()),
                "include_docs": True,
            },
        )
        r.raise_for_status()
        return {row["doc"]["_id"]: row["doc"] for row in r.json()["rows"]}

    def _upload_docs_to_couchdb(self, docs):
        """
        Upload a large number of documents to couchdb

        This takes a list of documents to be uploaded, and returns the status of
        them (which needs to be checked by the caller of this function. If there
        is a non-success status given back by couchdb, this raises it.
        """
        url = f"{self.base_url}/{self.project}/_bulk_docs"
        r = requests.post(
            url,
            auth=self.auth_token,
            json={
                "docs": docs,
            },
        )
        # pprint(r.json())
        r.raise_for_status()
        return r.json()

    def _get_document_from_couchdb(self, document_id):
        """
        Get a single document from coucbdb by its id.
        """
        url = f"{self.base_url}/{self.project}/{document_id}"
        r = self.make_request_get(url)
        # r = requests.get(url, auth=(self.user, self.token))
        # r.raise_for_status()
        return r.json()

    def _upload_document_to_couchdb(self, doc):
        """
        Upload a single document to couchdb
        """
        # pprint(doc)
        logging.debug(pformat(doc))
        url = f'{self.base_url}/{self.project}/{doc["_id"]}'
        r = requests.put(url, auth=self.auth_token, json=doc)
        r.raise_for_status()
        return r.json()

    def update_record_reference(self, record_id, base_revision_ids, new_revision_id):
        """
        Update record details to reference new revision
        """
        # We assume the following:
        # 1. The record has not been deleted at the couchdb level (records not
        #    handle deletion, revisions do).
        # 2. There are no intermediate revisions, that is `new_revision_id` is a
        #    child
        record = self._get_document_from_couchdb(record_id)
        revisions = set(record["revisions"])
        heads = set(record["heads"])
        logging.debug(pformat(revisions))
        logging.debug(pformat(base_revision_ids))
        if not revisions >= set(base_revision_ids):
            # checking if base revision ids is a strict subset. This seems... wrong?
            # @aragilar double check your logic. I've added a >= so this doesn't fail,
            #   but I'm not quite sure what you're cehcking here. I also had to cast
            #   base_revision_ids because set > list fails.
            raise ValueError("The base revisions are not all existing revisions")
        revisions.add(new_revision_id)
        for rev_id in base_revision_ids:
            heads.discard(rev_id)
        heads.add(new_revision_id)
        record["revisions"] = sorted(list(revisions))
        record["heads"] = sorted(list(heads))

        self._upload_document_to_couchdb(record)

    def update_existing_record(self, new_revision, new_avps):
        """
        Update a record with new data. This assumes that the revision has
        already been set up with new ids
        """
        failures = []
        logging.debug(new_avps)
        for result in self._upload_docs_to_couchdb(new_avps):
            # BBS
            logging.debug(f"uploaded avp {result}")
            if "error" in result:
                failures.append(result)
        if failures:
            raise RuntimeError("Some AVPs failed to upload", failures=failures)

        self._upload_document_to_couchdb(new_revision)
        logging.info("uploaded revision")
        self.update_record_reference(
            new_revision["record_id"],
            new_revision["parents"],
            new_revision["_id"],
        )

    def flatten_records(
        self, hide_empty=True, per_field_users=False, external_attachments=True
    ):
        """
        Gets all records from a FAIMS3 CouchDB instance.

        Given a faims object produced by the faims3couchdb class, flatten and get
        the latest avps for all record types.
        """

        # TODO remove empty cols for uncertainty, anntoations
        # Remove per-field user details (toggleable)

        records = self.fetch_records_for_roundtrip()
        dataframes = {}
        attachments = []
        shapes = {}

        for faims_record_key in records:
            record_list = []
            # print(f"{faims_record_key=}")
            record_name = self.record_type_names[faims_record_key]
            record = records[faims_record_key]
            dataframe = pandas.DataFrame()

            for key in record:
                # logging.debug(key)
                # try:
                identifier = record[key]["metadata"].get("identifier") or key

                for item in record[key]:
                    # logging.debug((item, pformat(record[key][item].keys())))
                    for item_key in [
                        "conflict_history",
                        "newest_avp_id",
                        "record_id",
                        "element",
                        "label",
                        "type",
                    ]:
                        if item == "metadata":
                            record[key]["metadata"]["record_name"] = record_name
                            if record[key]["metadata"]["identifier"]:
                                identifier = record[key]["metadata"]["identifier"]

                            record[key]["metadata"]["updates"] = str(
                                record[key]["metadata"]["updates"]
                            )
                            if "relationship" in record[key]["metadata"]:
                                logging.debug(record[key]["metadata"])
                            # logging.debug(pformat(record[key]["metadata"]))
                            if "parents" in record[key]["metadata"]:
                                del record[key]["metadata"]["parents"]
                        else:
                            if not per_field_users and "metadata" in record[key][item]:
                                del record[key][item]["metadata"]
                            if (
                                "in_conflict" in record[key][item]
                                and not record[key][item]["in_conflict"]
                            ):
                                del record[key][item]["in_conflict"]

                            if item_key in record[key][item]:
                                del record[key][item][item_key]
                            if isinstance(
                                record[key][item].get("data", {}).get("value"), list
                            ):
                                if (
                                    record[key][item]["data"]["value"]
                                    and "record_label"
                                    in record[key][item]["data"]["value"][0]
                                ):
                                    new_data = []
                                    for sub_item in record[key][item]["data"]["value"]:
                                        new_data.append(
                                            sub_item.get(
                                                "record_label", "label_unknown"
                                            )
                                        )
                                    record[key][item]["data"]["value"] = new_data
                                if not record[key][item]["data"]["value"]:
                                    record[key][item]["data"]["value"] = None

                            if isinstance(
                                record[key][item].get("data", {}).get("value"), dict
                            ):
                                if "geometry" in record[key][item]["data"]["value"]:
                                    orig = record[key][item]["data"]["value"].copy()
                                    orig_json = geojson.loads(json.dumps(orig))
                                    orig_json["id"] = identifier
                                    orig_json["properties"]["title"] = identifier
                                    orig_json["record_id"] = key
                                    # logging.debug(orig_json)
                                    if record_name not in shapes:
                                        shapes[record_name] = {}
                                    if item not in shapes[record_name]:
                                        shapes[record_name][item] = []

                                    shapes[record_name][item].append(orig_json)

                                    geo_shape = shape(orig["geometry"])
                                    record[key][item]["data"]["value"] = {}
                                    record[key][item]["data"]["value"][
                                        "geojson"
                                    ] = geojson.dumps(orig_json)
                                    record[key][item]["data"]["value"][
                                        "wkt"
                                    ] = geo_shape.wkt

                                    record[key][item]["data"]["value"][
                                        "y_latitude"
                                    ] = orig["geometry"]["coordinates"][1]
                                    record[key][item]["data"]["value"][
                                        "x_longitude"
                                    ] = orig["geometry"]["coordinates"][0]
                                    record[key][item]["data"]["value"][
                                        "accuracy"
                                    ] = orig["properties"]["accuracy"]

                                    record[key][item]["data"]["value"][
                                        "timestamp"
                                    ] = datetime.datetime.fromtimestamp(
                                        orig["properties"].get("timestamp", 0) / 1000.0,
                                        LOCAL_TIMEZONE,
                                    )

                                elif "geojson" in record[key][item]["data"]["value"]:
                                    pass
                                elif (
                                    "record_label" in record[key][item]["data"]["value"]
                                ):
                                    record[key][item]["data"]["value"] = record[key][
                                        item
                                    ]["data"]["value"]["record_label"]
                                else:
                                    record[key][item]["data"]["value"] = pformat(
                                        record[key][item]["data"]["value"]
                                    )

                            # if record[key][item].get("faims_attachments"):
                            # logging.debug((item, record[key][item].keys()))

                            if external_attachments and record[key][item].get(
                                "attachments"
                            ):
                                # logging.debug(identifier)
                                record_attachments = record[key][item]["attachments"]
                                record[key][item]["attached_files"] = []
                                counter = defaultdict(int)

                                for attachment in record_attachments:
                                    orig_filename = attachment["filename"]
                                    header, file = attachment["file"].split(",")
                                    header = re.sub(
                                        r"data:", r"", re.sub(";base64", "", header)
                                    )

                                    if orig_filename and "." in orig_filename:
                                        extension = ".".join(
                                            [
                                                slugify(
                                                    x,
                                                    max_length=64,
                                                    allow_unicode=True,
                                                    lowercase=False,
                                                )
                                                for x in orig_filename.split(".")
                                            ]
                                        )
                                        extension = f".{extension}"
                                        # logging.debug((orig_filename, extension))
                                        # base_filename = f".{orig_filename}"
                                    else:
                                        extension = guess_extension(header)
                                        # base_filename = ""
                                    counter[key] += 1
                                    # logging.debug(
                                    #     f"*****attach****\n,{key},{identifier},{counter[key]},{extension}"
                                    # )
                                    #
                                    # attachment_path.mkdir(parents=True, exist_ok=True)
                                    record_nametype = f"{record_name}"
                                    attachment = {
                                        "path": f"{slugify(record_nametype, max_length=128, allow_unicode=True, lowercase=False)}/{slugify(item, max_length=128, allow_unicode=True, lowercase=False)}",
                                        "filename": f"{slugify(identifier, max_length=128, allow_unicode=True, lowercase=False)}.{slugify(item, max_length=64, allow_unicode=True, lowercase=False)}.{counter[key]}{extension}",
                                        "data": base64.standard_b64decode(file),
                                    }
                                    record[key][item]["attached_files"].append(
                                        str(
                                            f"{attachment['path']}/{attachment['filename']}"
                                        )
                                    )
                                    attachments.append(attachment)
                                if not record[key][item]["attached_files"]:
                                    del record[key][item]["attached_files"]
                            if "attachments" in record[key][item]:
                                del record[key][item]["attachments"]

                    # logging.debug(pformat(record[key][item]))
                record_list.append(record[key])

            df = pandas.json_normalize(record_list)
            df = df.rename(columns=lambda x: re.sub(".data.value", "", x))
            # df.set_index("metadata.identifier", inplace=True)
            if hide_empty:
                anno_map = df.columns[df.columns.str.endswith("annotation")]
                anno_isna = df[anno_map].isna().all(axis=0)
                anno_map = anno_map[
                    anno_isna == True
                ]  # Ok, this is only the list of droppable columns. Argh.
                # logging.debug(annoisna)
                # logging.debug(annomap)
                df.drop(columns=anno_map, inplace=True)

                # df.drop(columns=annoisna, axis=1)
                cert_map = df.columns[df.columns.str.endswith("certainty")]
                cert_isfalse = df[cert_map].eq(False).all(axis=0)
                cert_map = cert_map[cert_isfalse]
                # logging.debug(certmap)
                # logging.debug(certisfalse)
                df.drop(columns=cert_map, inplace=True)
                # colmap = annomap | certmap

                # https://stackoverflow.com/questions/24775648/element-wise-logical-or-in-pandas
                # https://stackoverflow.com/questions/46864740/selecting-a-subset-using-dropna-to-select-multiple-columns
                # logging.debug(colmap)
                # BBS Continue. Hideempty needs to match only annotations or certainty
                # df = df.filter(regex=r".*annotation$", axis=1).dropna(how="all", axis=1)
                # df = df.T[~df[certmap].eq(False).all()].T

            # except KeyError:
            #     logging.error(f"KeyError {key}")

            dataframes[record_name] = df

        return (dataframes, attachments, shapes)

        #         for key in record_avps:
        #             avp = record_avps[key]

        #             avp_element = type_rev_lookup[avp["_id"]]

        #             avp_type = self.field_mapping.get(
        #                 avp_element, avp_element
        #             )  # try to lookup the human element name, if not return the generic element internal name
        #             print(f"\n\n{avp_element}")
        #             pprint(avp)

        #             if avp_element in self.multivalued_fields:
        #                 multi_element = {}
        #                 cols = [
        #                     x["value"]
        #                     for x in self.multivalued_fields[avp_element]["values"]
        #                 ]
        #                 for col in cols:
        #                     # print(f"{avp_type} {col}")

        #                     value = None
        #                     # record_keys.add(f"{avp_type}_{col}")
        #                     if col in avp["data"]:
        #                         value = col
        #                     multi_element[col] = value
        #                     self.record_fieldnames_odict[record_type][
        #                         f"{avp_type}_{col}"
        #                     ] = f"{avp_element}::flattened"
        #                 record[avp_type] = multi_element
        #             elif "data" in avp and avp.get("type", "") == "faims-pos::Location":
        #                 # print("geojson")
        #                 record[avp_type] = json.dumps(avp["data"])
        #             elif "data" in avp:
        #                 record[avp_type] = avp["data"]
        #             else:

        #                 record_iter.write(
        #                     f"Flattenerror: No data in {avp['_id']}, {avp_element}, {avp_type}"
        #                 )
        #                 print(avp)
        #                 raise ValueError
        #             # TODO Build in annotation dumping, this is temporary so I can handle annotation columns
        #             # if "annotations" in avp:
        #             #     for key, annotation in avp['annotations'].items():
        #             #         if annotation:
        #             #             pprint(avp)
        #             #             pprint(key)
        #             #             pprint(annotation)
        #             #             annotation_column_name = self.field_mapping[f"{avp_element} {key}"]
        #             #             record[annotation_column_name] = annotation

        #             # print("AVP", avp_type, avp['data'])

        #             record_union = record | flatten(record)

        #             self.record_fieldnames_odict[record_type][avp_type] = avp_element

        #             for key in record_union.keys():
        #                 if key not in self.record_fieldnames[record_type]:
        #                     self.record_fieldnames[record_type].append(key)
        #                 # if avp_element in self.record_fieldnames_odict[record_type]:
        #                 #     self.record_fieldnames_odict[record_type][avp_element].insert(0,key)
        #                 # else:
        #                 #     self.record_fieldnames_odict[record_type] = {avp_element:[key]}

        #             # self.record_fieldnames[record_type] = list(self.record_fieldnames[record_type]) + list(record_union.keys())
        #             # print(record_type)
        #             # pprint(self.record_fieldnames[record_type])
        #             # pprint(record_union.keys())
        #         self.record_count[record_type] += 1
        #         records[record_type].append(
        #             record_union
        #         )  # 3.9 feature of dict union operator. Works exactly the way I wanted it to.

        # self.records = records
        # return records

    def fetch_records_for_roundtrip(
        self, match_uuids=[], disable_progress_bars=False, include_attachments=True
    ):
        """
        Gets all records from a FAIMS3 CouchDB instance.

        Given a faims object produced by the faims3couchdb class, flatten to JSON and get
        the latest avps for all record types.
        """

        records = {}

        logging.info(f"Exporting: {self.project}")
        record_iter = tqdm.tqdm(
            self.get_records(), desc=f"JSON records", disable=disable_progress_bars
        )

        # new_revision_id = str(uuid4())
        for faims_record in record_iter:
            # print(faims_record)
            if match_uuids and faims_record["_id"] not in match_uuids:
                continue
            # record_iter.write(pformat(f"{faims_record=}"))
            record_type = faims_record["type"]
            created = faims_record["created"]
            # logging.debug(created)
            created_by = faims_record["created_by"]
            record_id = faims_record["_id"]
            # pprint(faims_record)

            # print(record_type)
            # sys.exit(0)
            try:
                all_revisions = self.get_all_revisions_for_record(faims_record).items()
            except Exception as e:
                continue

            try:
                revisions = self.get_head_revisions_for_record(faims_record).items()
            except Exception as e:
                continue

            """
            TODO

            1. Exporter needs to obey "delete this record" (and figure out how it's being set)
            2. Make sure each line is one and only one uuid
                2a. That conflicts in avps are listed in each avp instead
                2b. choose a value for each avp if there is only one
                2c. show username, timestamp, value for each avp
            3. Export label as part of each avp
            """

            revision_authordate = OrderedDict()
            revision_bykey = {}
            isdeleted = False
            for revision_key, revision in all_revisions:
                # print(revision_key)
                revision_authordate[revision["created"]] = {
                    "created_by": revision["created_by"],
                    "created_at": revision["created"],
                    "revision_key": revision_key,
                    "deleted": revision.get("deleted", False),
                }
                revision_bykey[revision_key] = {
                    "created_by": revision["created_by"],
                    "created_at": revision["created"],
                }
                if "relationship" in revision:
                    revision_bykey[revision_key]["relationship"] = revision[
                        "relationship"
                    ]
            for revision_key, revision in revisions:
                isdeleted = revision.get("deleted", False)
            if isdeleted and not self.include_deleted:
                continue
            identifier = ""
            # Revisions... should be only one.
            for revision_key, revision in revisions:
                record = OrderedDict()

                updated_at = revision_bykey[revision_key]["created_at"]
                updated_by = revision_bykey[revision_key]["created_by"]
                record["metadata"] = {
                    "identifier": None,
                    "record_type": record_type,
                    "updated_at": updated_at,
                    "updated_by": updated_by,
                    "in_conflict": False,
                    "deleted": isdeleted,
                    "parents": [],
                    "record_id": faims_record["_id"],
                }

                if revision_bykey[revision_key].get("relationship"):
                    if "parent" in revision_bykey[revision_key]["relationship"]:
                        this_reln = revision_bykey[revision_key]["relationship"][
                            "parent"
                        ]
                        # logging.debug(pformat(this_reln))

                        record["metadata"]["relationship_verb"] = this_reln[
                            "relation_type_vocabPair"
                        ][0]
                        record["metadata"]["relationship_parent_record_hrid"] = None
                        record["metadata"]["relationship_parent_record_form"] = None
                        record["metadata"]["relationship_parent_record_id"] = this_reln[
                            "record_id"
                        ]
                        record["metadata"]["relationship_parent_field_id"] = this_reln[
                            "field_id"
                        ]
                    elif "linked" in revision_bykey[revision_key]["relationship"]:
                        # BBS Resume
                        logging.debug(
                            pformat(revision_bykey[revision_key]["relationship"])
                        )

                        this_reln = revision_bykey[revision_key]["relationship"][
                            "linked"
                        ]
                        # logging.debug(pformat(this_reln))

                        record["metadata"]["relationship_verb"] = this_reln[
                            "relation_type_vocabPair"
                        ][0]
                        record["metadata"]["relationship_linked_record_hrid"] = None
                        record["metadata"]["relationship_linked_record_form"] = None
                        record["metadata"]["relationship_linked_record_id"] = this_reln[
                            "record_id"
                        ]
                        record["metadata"]["relationship_linked_field_id"] = this_reln[
                            "field_id"
                        ]

                # get_all_revisions_for_record in case historical versions are indicated
                # print("revision", revision_key)
                record["metadata"]["parents"].append(revision_key)
                record["metadata"]["updates"] = revision_authordate
                type_rev_lookup = {}
                for avp_type in revision["avps"]:
                    type_rev_lookup[revision["avps"][avp_type]] = avp_type
                # print(f"foo {record_type}")
                # pprint(type_rev_lookup)

                record_avps = self.get_all_avps_for_revision(revision)
                # record_keys = dict.from_keys(['record_type', 'created_by', 'created_at'])
                for key in record_avps:
                    avp = record_avps[key]

                    # print(avp)
                    avp_id = avp["_id"]
                    avp_element = type_rev_lookup[avp_id]

                    avp_type = self.field_mapping.get(
                        avp_element, avp_element
                    )  # try to lookup the human element name, if not return the generic element internal name

                    avp_field_metadata = self.field_metadata.get(avp_element)
                    # logging.debug(pformat(avp_field_metadata, width=200))

                    if avp["type"] == "??:??":
                        continue
                    # pprint(avp_field_metadata)

                    # hierarchy = self.element_hierarchy[avp_element]
                    # form = hierarchy['viewset']
                    # view = hierarchy['view']

                    try:
                        # print(avp)
                        # print(avp_type)
                        # logging.debug(avp_type)

                        # if avp_type == "FIP Site ID":
                        if avp_field_metadata and avp_field_metadata.get(
                            "component-parameters", {}
                        ).get("hrid", False):
                            identifier = avp["data"]
                            record["metadata"]["identifier"] = avp["data"]
                            self.identifiers[faims_record["_id"]] = identifier

                        if (
                            avp_field_metadata
                            and avp_field_metadata.get("component-name")
                            == "TemplatedStringField"
                            and "hridFORM"
                            in avp_field_metadata.get("component-parameters", {}).get(
                                "id"
                            )
                        ):
                            identifier = avp["data"]
                            record["metadata"]["identifier"] = avp["data"]
                            self.identifiers[faims_record["_id"]] = identifier
                        # pprint(revision_authordate[avp['revision_id']])
                        # logging.debug(pformat(avp))
                        self.forms_from_record_id[faims_record["_id"]] = record_type
                        record[avp_type] = {
                            "record_id": faims_record["_id"],
                            "newest_avp_id": avp["_id"],
                            "element": avp_element,
                            "label": avp_type,
                            # 'form':form,
                            # 'view':view,
                            #'new_revision_id':new_revision_id,
                            "type": avp["type"],
                            "data": {
                                "value": avp["data"],
                                "annotation": avp["annotations"]["annotation"] or None,
                                "uncertainty": avp["annotations"]["uncertainty"],
                            },
                            "metadata": revision_bykey[avp["revision_id"]],
                            "attachments": [],
                            "conflict_history": {},
                            "in_conflict": False,
                        }

                        # if record_id in records.get(record_type,{}):
                        #     old_data = records[record_type][record_id][avp_type]
                        #     if record[avp_type]["data"] != old_data["data"] or record[avp_type]["metadata"] != old_data["metadata"]:
                        #         print(record[avp_type]["data"], old_data["data"])
                        #         record[avp_type]['conflict_history'].append({"data":old_data['data'],
                        #                                                      "metadata":old_data['metadata']
                        #                                                     })

                        # Tranche 1.55 attachments
                        if include_attachments:
                            for attachment in avp.get("faims_attachments", {}):
                                # logging.debug(pformat(avp))
                                # logging.debug(identifier)
                                # logging.debug(attachment)
                                attach_url = f"{self.base_url}/{self.project}/{attachment['attachment_id']}/{attachment['attachment_id']}"
                                try:
                                    with requests.get(
                                        attach_url,
                                        auth=self.auth_token,
                                    ) as attach_get:
                                        attach_get.raise_for_status()
                                        file = f"data:{attach_get.headers['Content-Type']};base64,{base64.b64encode(attach_get.content).decode('utf-8')}"
                                        record[avp_type]["attachments"].append(
                                            {
                                                "filename": attachment["filename"],
                                                "file": file,
                                            }
                                        )
                                except requests.exceptions.HTTPError as e:
                                    logging.error(
                                        f"Could not fetch attachment for {attach_url}. Error: {e}\n"
                                    )
                            # Tranche 1 attachments
                            for attachment in avp.get("_attachments", {}):
                                # https://alpha.db.faims.edu.au
                                # project         /data-farmer_incentive_program_data_collection_notebook_for_service_provider_sp_id_mon_24_jan_2022_22_32_36_aedt-5433d34e-7d09-11ec-acbe-9beb1ca0af9d
                                # doc_id             /61d83be6-ddb2-4b10-9b37-49cdb0f6f253
                                # attachment key  /b942e745-c25c-4a59-a7d7-8de49df46add
                                # url =  f'{self.base_url}/{self.project}
                                attach_url = f"{self.base_url}/{self.project}/{avp['_id']}/{attachment}"
                                # print(attach_url)
                                with requests.get(
                                    attach_url,
                                    auth=self.auth_token,
                                ) as attach_get:
                                    attach_get.raise_for_status()
                                    file = f"data:{attach_get.headers['Content-Type']};base64,{base64.b64encode(attach_get.content).decode('utf-8')}"
                                    record[avp_type]["attachments"].append(
                                        {"filename": None, "file": file}
                                    )
                        record[avp_type]["conflict_history"][updated_at] = {
                            "created_by": updated_by,
                            "created_at": updated_at,
                            "data": record[avp_type]["data"].copy(),
                            "attachment_count": len(record[avp_type]["attachments"]),
                        }
                    except Exception as e:
                        traceback.print_exc()
                        record_iter.write(
                            f"No data in {avp['_id']}, {avp_element}, {avp_type}"
                        )
                        # sys.exit(1)

                # iterate this at the revision level
                if record_type not in records:
                    records[record_type] = {}
                if record_id in records.get(record_type):
                    extant_record = records[record_type][record_id].copy()
                    logging.debug(f"Conflict: {identifier}")
                    record["metadata"]["in_conflict"] = True
                    # pprint(records[record_type][record_id].keys())

                    # logging.debug(
                    #     pformat(
                    #         records[record_type][record_id]["metadata"]["updated_at"]
                    #     )
                    # )
                    # logging.debug((record["metadata"]["updated_at"]))

                    for key in record:
                        if "conflict_history" in record[key]:
                            records[record_type][record_id][key][
                                "conflict_history"
                            ].update(record[key]["conflict_history"])
                            record[key]["conflict_history"].update(
                                extant_record[key]["conflict_history"]
                            )
                        for datavaluetype in ["value", "annotation", "uncertainty"]:
                            if "data" not in record[key]:
                                continue
                            if (
                                records[record_type][record_id][key]["data"][
                                    datavaluetype
                                ]
                                is None
                                or records[record_type][record_id][key]["data"][
                                    datavaluetype
                                ]
                                == ""
                            ) and (
                                record[key]["data"][datavaluetype] is not None
                                and record[key]["data"][datavaluetype] != ""
                            ):
                                logging.debug(
                                    f"new -> old {key} {datavaluetype} = {record[key]['data'][datavaluetype]}"
                                )
                                records[record_type][record_id][key]["data"][
                                    datavaluetype
                                ] = record[key]["data"][datavaluetype]

                            if (
                                record[key]["data"][datavaluetype] is not None
                                or record[key]["data"][datavaluetype] == ""
                            ) and (
                                records[record_type][record_id][key]["data"][
                                    datavaluetype
                                ]
                                is not None
                                and records[record_type][record_id][key]["data"][
                                    datavaluetype
                                ]
                                != ""
                            ):
                                # logging.debug(
                                #     f"old -> new {key} {datavaluetype} = {extant_record[key]['data'][datavaluetype]}"
                                # )
                                record[key]["data"][datavaluetype] == extant_record[
                                    key
                                ]["data"][datavaluetype]

                            if (
                                record[key]["attachments"]
                                and not extant_record[key]["attachments"]
                            ):
                                logging.debug(f"new -> old, attachment {key}")
                                records[record_type][record_id][key][
                                    "attachments"
                                ] = record[key]["attachments"]

                            if (
                                extant_record[key]["attachments"]
                                and not record[key]["attachments"]
                            ):
                                logging.debug(f"old -> new, attachment {key}")
                                record[key]["attachments"] = records[record_type][
                                    record_id
                                ][key]["attachments"]

                    if (
                        extant_record["metadata"]["updated_at"]
                        < record["metadata"]["updated_at"]
                    ):
                        records[record_type][record_id] = record

                else:
                    records[record_type][
                        record_id
                    ] = record  # 3.9 feature of dict union operator. Works exactly the way I wanted it to.

                self.record_count[record_type] += 1
        for form in records:
            for key in records[form]:
                record = records[form][key]
                if "relationship_parent_record_id" in record["metadata"]:
                    records[form][key]["metadata"][
                        "relationship_parent_record_hrid"
                    ] = self.identifiers[
                        records[form][key]["metadata"]["relationship_parent_record_id"]
                    ]
                    records[form][key]["metadata"][
                        "relationship_parent_record_form"
                    ] = self.record_type_names[
                        self.forms_from_record_id[
                            records[form][key]["metadata"][
                                "relationship_parent_record_id"
                            ]
                        ]
                    ]
                    logging.debug(pformat(record["metadata"]))

        self.records = records
        return records

    def get_fetched_records(self):
        """
        Fetches and flattens if needed, otherwise returns records dict.
        """

        if self.records:
            return self.records
        else:
            return self.fetch_and_flatten_records()

    def fetch_project_metadata(self, metadata_key="project-metadata-"):
        """
        Fetches all docs in metadata- that start with the metadata_key, and
        then replaces _ with " " and sets self.project_metadata
        """
        project_metadata = {}
        url = f"{self.base_url}/{self.metadata}/_all_docs"
        r = requests.post(
            url,
            auth=self.auth_token,
            json={
                "include_docs": True,
            },
        )
        r.raise_for_status()
        for row in r.json()["rows"]:
            if metadata_key in row["id"]:
                clean_metadata_key = re.sub(
                    "_", " ", re.sub(metadata_key, "", row["id"])
                )
                # TODO handle files once we figure out what they are
                if row["doc"]["is_attachment"]:
                    tempdir = tempfile.mkdtemp()
                    # raise Exception("Help, not implemented!")
                    # print("Attachment?!")

                    # pprint(row)

                    # print(self.base_url) #https://testing.db.faims.edu.au/
                    # print(self.metadata) # metadata-demo_from_builder-b5f0015a-57a9-11ec-b8ff-33d8bb230b2a/
                    # row['id']            # project-metadata-attachments
                    # /6821_A_extinct-corr_adaptive_recom-comp.fits
                    attach_base_url = f'{self.base_url}/{self.metadata}/{row["key"]}'
                    for attachment in row["doc"]["_attachments"]:
                        attach_url = f"{attach_base_url}/{attachment}"
                        with requests.get(
                            attach_url, auth=self.auth_token, stream=True
                        ) as attach_get:
                            attach_get.raise_for_status()
                            # https://stackoverflow.com/a/16696317
                            with open(f"{tempdir}/{attachment}", "wb") as f:
                                for chunk in attach_get.iter_content(chunk_size=8192):
                                    f.write(chunk)
                            self.project_metadata_attachments[
                                attachment
                            ] = f"{tempdir}/{attachment}"

                else:
                    project_metadata[clean_metadata_key] = row["doc"]["metadata"]
        self.project_metadata = project_metadata
        # pprint(project_metadata)
