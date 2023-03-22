#!/usr/bin/env python3


"""
This adds a bunch of helpers for importing back into couchdb. Below is the
flow I envision that would happen. The bits that are not included are around
how to go from formats like CSV (where information may be lost) to getting
the necessary metadata for import.

To use these helpers, lets assume we have a single field "email", and there is
an existing record we want to work with, that has id "abc123" (obviously not
real uuid4, but it'll be easier to read in the example). The changes are on
top of revision "old0".

First, we need to create a new revision id (a new uuidv4), lets call
that "def456".

Next, we need to create a new attribute-value pair a.k.a  AVP (any change in
the values implies creating a new AVP).

We call `create_new_avp` like so: 
```python 
new_avp = create_new_avp( data=new_email, 
                         revision_id="def456", 
                         record_id="abc123", 
                         annotations={}, 
                         type="faims-core::Email" ) ``` 
Note that annotations will vary depending
on the project (it's unclear to me how we're planning on handling the
roundtrip of those), and type is currently just a label. The safest thing
would to use the values received from the initial export. 

Once that's done, we need to create our new revision, by using
`create_new_revision`: 
```python 
new_revision = create_new_revision( avps={"email": new_avp["_id"], 
                                         new_id="def456", 
                                         parents=["old0"], 
                                         record_id="abc123", 
                                         type="proj::Email", 
                                         created_by="some user",
                                         created="1970-01-01" ) 
```

Finally, we setup the couchdb helper (lets call it `helper`), and use
`helper.update_existing_record(new_revision,[new_avp])`
"""

from faims3couchdb import CouchDBHelper, create_new_avp, create_new_revision
from faims3records import FAIMS3Record
from pprint import pformat
import jsonlines
import logging
from pprint import pprint
import json
from uuid import uuid4
import shutil
import os
from pathlib import Path
import re
from mimetypes import guess_extension, guess_type
from collections import defaultdict
import base64
from slugify import slugify
from geojson import FeatureCollection
import geojson
from pandas.api.types import is_datetime64_ns_dtype
import numpy as np
import simplekml

OUTPUT_DIR = Path("output")


def export_csv(
    user,
    token,
    base_url,
    project_key,
    inline_attachments,
    external_attachments,
    bearer_token=None,
):
    # shutil.rmtree(OUTPUT_DIR, ignore_errors=True)
    clean_url = slugify(base_url)
    project_path = OUTPUT_DIR / f"{clean_url}+{project_key}"
    # print(base_url)
    faims = CouchDBHelper(
        user=user,
        token=token,
        base_url=base_url,
        project_key=project_key,
        bearer_token=bearer_token,
    )

    records, attachments, shapes = faims.flatten_records(iterator="notebook")
    if records:
        project_path.mkdir(parents=True)
        for key, dataframe in records.items():
            # dataframe.set_index("metadata.identifier")
            if dataframe["metadata.identifier"].is_unique:
                dataframe.set_index("metadata.identifier", inplace=True)
            form_path = project_path / slugify(key, lowercase=False)
            form_path.mkdir(parents=True, exist_ok=True)
            dataframe.to_csv(form_path / f"{slugify(key, lowercase=False)}.csv")
            dataframe.to_json(form_path / f"{slugify(key, lowercase=False)}.json")
            # Excel doesn't do timezones
            # for col in dataframe.dtypes:
            #     if is_datetime64_ns_dtype(col):
            #         logging.debug(col)
            #         dataframe[col.index] = dataframe[col].dt.tz_localize(None)
            # https://stackoverflow.com/a/66699516
            date_columns = dataframe.select_dtypes(
                include=["datetime64[ns, UTC]"]
            ).columns
            for date_column in date_columns:
                dataframe[date_column] = dataframe[date_column].dt.date
            dataframe.to_excel(
                form_path / f"{slugify(key, lowercase=False)}.xlsx",
                engine="xlsxwriter",
            )
        for attachment in attachments:
            filename = attachment["filename"]
            data = attachment["data"]
            attachment_path = project_path / attachment["path"]
            if data:
                attachment_path.mkdir(parents=True, exist_ok=True)
            with open(attachment_path / filename, "wb") as attach:
                attach.write(data)
        for form, shape in shapes.items():
            form_df = records[form]
            form_df.set_index("metadata.record_id", inplace=True)
            form_dict = json.loads(form_df.to_json(orient="index"))
            # logging.debug(pformat(form_dict))
            for item_name, item in shape.items():
                for entry in item:
                    entry["record_data"] = form_dict[entry["record_id"]]
                shape_path = project_path / slugify(form, lowercase=False)
                shape_path.mkdir(parents=True, exist_ok=True)
                item_FeatureCollection = FeatureCollection(item)
                with open(
                    shape_path / f"{slugify(item_name, lowercase=False)}.geojson", "w"
                ) as geojson_file:
                    geojson.dump(
                        item_FeatureCollection, geojson_file, allow_nan=True, indent=2
                    )
                # https://stackoverflow.com/a/71116608
                kml = simplekml.Kml()
                for feature in item_FeatureCollection["features"]:
                    geom = feature["geometry"]
                    geom_type = geom["type"]
                    if geom_type == "Polygon":
                        kml.newpolygon(
                            name=feature["id"],
                            description=json.dumps(
                                form_dict[feature["record_id"]], indent=2
                            ),
                            outerboundaryis=geom["coordinates"][0],
                        )
                    elif geom_type == "LineString":
                        kml.newlinestring(
                            name=feature["id"],
                            description=json.dumps(
                                form_dict[feature["record_id"]], indent=2
                            ),
                            coords=geom["coordinates"],
                        )
                    elif geom_type == "Point":
                        kml.newpoint(
                            name=feature["id"],
                            description=json.dumps(
                                form_dict[feature["record_id"]], indent=2
                            ),
                            coords=[geom["coordinates"]],
                        )
                    else:
                        logging.error(f"ERROR: unknown type: {geom_type}")
                kml.save(shape_path / f"{slugify(item_name, lowercase=False)}.kml")
                # logging.debug(item)

    # print("records")
    # for form in records:
    #     # pprint(form)
    #     for record_key in records[form]:
    #         record = records[form][record_key]
    #         jsonl_record = {"metadata": {}}
    #         identifier = "unknown"
    #         for key in record:
    #             if key == "metadata":
    #                 for item in record[key]:
    #                     # pprint(record[key][item])
    #                     jsonl_record["metadata"][item] = record[key][item]
    #             else:
    #                 # form = record[key]['form']
    #                 # view = record[key]['view']
    #                 # if form not in jsonl_record:
    #                 #     jsonl_record[form] = {view:{}}
    #                 # if view not in jsonl_record[form]:
    #                 #     jsonl_record[form][view] = {}

    #                 if "hrid" in record[key]["element"]:
    #                     # pprint(record[key]['data'])
    #                     identifier = re.sub(
    #                         r"[^A-Za-z0-9.+-]+",
    #                         "_",
    #                         f"{record[key]['data']['value']}+{record[key]['metadata']['created_at']}+{record[key]['metadata']['created_by']}",
    #                     )

    #                 # print(key, record[key]['element'], record[key]['data'])
    #                 # pprint(record)
    #                 jsonl_record[record[key]["element"]] = record[key]["data"]
    #                 jsonl_record[record[key]["element"]].update(record[key]["metadata"])
    #                 jsonl_record[record[key]["element"]]["conflict_history"] = record[
    #                     key
    #                 ]["conflict_history"]
    #                 jsonl_record[record[key]["element"]]["attachments"] = []
    #                 jsonl_record[record[key]["element"]]["label"] = record[key]["label"]
    #                 if inline_attachments and record[key]["attachments"]:
    #                     jsonl_record[record[key]["element"]] = {
    #                         "data": record[key]["data"],
    #                         "attachments": [record[key]["attachments"]],
    #                     }

    #                     jsonl_record[record[key]["element"]]["data"].update(
    #                         record[key]["metadata"]
    #                     )
    #                 if external_attachments and record[key]["attachments"]:
    #                     # pprint(record[key])
    #                     counter = defaultdict(int)
    #                     for attachment in record[key]["attachments"]:
    #                         header, file = attachment.split(",")
    #                         header = re.sub(
    #                             r"data:", r"", re.sub(";base64", "", header)
    #                         )
    #                         extension = guess_extension(header)
    #                         counter[key] += 1
    #                         # print(key, identifier, counter[key], extension)
    #                         attachment_path = project_path / key
    #                         attachment_path.mkdir(parents=True, exist_ok=True)
    #                         filename = f"{identifier}.{counter[key]}{extension}"
    #                         jsonl_record[record[key]["element"]]["attachments"].append(
    #                             str(f"{key}/{filename}")
    #                         )
    #                         with open(attachment_path / filename, "wb") as attach:
    #                             attach.write(base64.standard_b64decode(file))

    #         with jsonlines.open(project_path / "output.jsonl", mode="a") as writer:
    #             writer.write(jsonl_record)
    # for record_type in records:
    #     with open(project_path / f"{record_type}.json","w") as sample:
    #         json.dump(records[record_type], sample, indent=2)


if __name__ == "__main__":
    export_csv()
