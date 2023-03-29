import ipywidgets as widgets
from ipywidgets import Button, Layout
import jwt
import base64
import json
from pprint import pprint, pformat


import requests
from faims3couchdb import CouchDBHelper, create_new_avp, create_new_revision
from faims3records import FAIMS3Record
from export_csv import export_csv
from pathlib import Path
from slugify import slugify
import datetime
import logging
import shutil
import os
from tqdm.auto import tqdm
import zipfile
from IPython.display import FileLink, HTML, display
import IPython
from notebook import notebookapp
import tarfile
import logging
import time
import os
from github import Github
import traceback
import textwrap
import functools


class BearerAuth(requests.auth.AuthBase):
    # https://stackoverflow.com/a/58055668
    def __init__(self, token):
        self.token = token

    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token
        return r


log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


OUTPUT = Path("output")
FORMAT = (
    "[%(asctime)s %(filename)s->%(funcName)s():%(lineno)s]%(levelname)s: %(message)s"
)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

logging.basicConfig(format=FORMAT, level=logging.WARNING)

out = widgets.Output(layout={"border": "0px solid black"})
out2 = widgets.Output(layout={"border": "0px solid black"})
out_url = widgets.Output(layout={"border": "0px solid black"})

layout_hidden = widgets.Layout(display="none")
layout_visible = widgets.Layout(display="block")

desc_style = {"description_width": "initial"}


envvars = []
for name, value in os.environ.items():
    envvars.append("<li><pre>{0}: {1}</pre></li>".format(name, value))
display(
    HTML(
        f"<details><summary>Debug Variables</summary><ul>{' '.join(envvars)}</ul></details>"
    )
)


bearer_token = widgets.Text(
    value="",
    placeholder="",
    description="Bearer Token:",
    disabled=False,
    style=desc_style,
)


show_button = widgets.Button(
    description="Show bearer token text field",
    layout=layout_hidden,
)


github_url_text = widgets.Text(
    description="Url of Notebook on Github",
    # value="https://github.com/FAIMS/FAIMS3-notebook-template",
    # layout=layout_hidden,
    style=desc_style,
    continuous_update=False,
    layout=Layout(width="60%", height="50px"),
    placeholder="https://github.com/FAIMS/FAIMS3-notebook-template",
)


readme_url = ""
citation_url = ""


def get_exporter_metadata():
    token = decode_token()
    notebook_id = notebook_select.value["notebook"]["_id"]
    auth_token = BearerAuth(token["jwt_token"])

    url = f"{token['base_url']}/metadata-{notebook_id}/exporter-metadata"

    r = requests.get(url, auth=auth_token)
    # r.raise_for_status()
    doc = r.json()
    return doc


@out_url.capture()
def validateURL(change):
    urlsplit = change.new.split("/")
    token = decode_token()
    notebook_id = notebook_select.value["notebook"]["_id"]
    auth_token = BearerAuth(token["jwt_token"])
    url = f"{token['base_url']}/metadata-{notebook_id}/exporter-metadata"
    doc = get_exporter_metadata()

    if change.new != doc.get("repository") and notebook_id and len(urlsplit) > 3:
        try:
            if "github.com" in urlsplit[2]:
                organisation = urlsplit[3]
                repo_name = urlsplit[4]
                display(HTML(f"<li>Parsed {organisation=} {repo_name=}</li>"))
                g = Github()
                repo = g.get_repo(f"{organisation}/{repo_name}")
                readme = repo.get_contents("README.md")
                content = textwrap.shorten(
                    base64.b64decode(readme.content).decode("utf-8"), width=100
                )
                display(
                    HTML(
                        f"Fetching readme from {repo.name}: <div><pre>{content}</pre></div>"
                    )
                )
                if readme:
                    if "not_found" in doc.get("error", ""):
                        r2 = requests.put(
                            url,
                            auth=auth_token,
                            json={
                                "repository": change.new,
                                "organisation": organisation,
                                "repo_name": repo_name,
                            },
                        )
                        r2.raise_for_status()
                        print("Created repository url in database")
                    else:
                        r2 = requests.put(
                            url,
                            auth=auth_token,
                            json={
                                "repository": change.new,
                                "organisation": organisation,
                                "repo_name": repo_name,
                                "_rev": doc["_rev"],
                            },
                        )
                        r2.raise_for_status()

                        print("Updated repository url to database")

                return True
        except Exception:
            display(HTML(f"<li>unable to parse github url.</li>"))
            print(traceback.print_exc())
            return False
    return False


github_url_text.observe(validateURL, "value")


@out.capture()
def in_notebook():
    try:
        from IPython import get_ipython

        print(get_ipython().config)
        if "IPKernelApp" not in get_ipython().config:  # pragma: no cover
            return False
    except ImportError:
        return False
    except AttributeError:
        return False
    return True


@out.capture()
def decode_token():
    try:
        token = json.loads(base64.b64decode(bearer_token.value))
        token["base_url"] = token["userdb"].replace("/people", "")
        token.update(
            jwt.decode(
                token["jwt_token"], token["public_key"], algorithms=[token["alg"]]
            )
        )
        # pprint(token)
        return token
    except:
        print("Unable to decode token")


def visible_bearer(button):
    bearer_token.value = ""
    bearer_token.layout = layout_visible
    show_button.layout = layout_hidden


def check_for_valid(change):
    try:
        token = json.loads(base64.b64decode(bearer_token.value))
        validate_database_connection(validate_button)
    except:
        pass


@out.capture()
def validate_database_connection(button):
    out.clear_output()
    out2.clear_output()
    if bearer_token.value:
        print("Validating...")
        try:
            token = decode_token()
            # print(token)
            jwt_token = jwt.decode(
                token["jwt_token"], token["public_key"], algorithms=[token["alg"]]
            )
            print(f"Hello: {token['name']} ({token['sub']}) on {token['base_url']}")

            # out.append_stdout(token)

            bearer_token.layout = layout_hidden
            show_button.layout = layout_visible
            notebooks = prepare_select(list_notebooks())
            # print(notebooks)
            notebook_select.options = notebooks
            notebook_select.observe(get_notebook_readme, names="value")
            get_notebook_readme(change={"new": notebook_select.value})
            display(notebook_select)
            # display(overwrite_checkbox)
            # display(list_checkbox)
            display(github_url_text)
            display(out_url)
            display(export_button)
            # list_notebooks()
            display(out2)
        except Exception as e:
            print(f"Please tell brian@faims.edu.au: {e}")
            bearer_token.layout = layout_visible
            show_button.layout = layout_hidden
            bearer_token.value = ""


validate_button = widgets.Button(
    description="Validate Bearer Token",
    disabled=False,
    layout=Layout(width="20%", height="50px"),
    button_style="",  # 'success', 'info', 'warning', 'danger' or ''
    tooltip="Click here to connect to the server in order to validate the Bearer Token",
    icon="check",  # (FontAwesome names without the `fa-` prefix)
)


@out2.capture()
def list_notebooks():
    token = decode_token()

    url = f"{token['base_url']}/projects/_find"
    r = requests.post(
        url,
        headers={"Authorization": token["jwt_token"]},
        json={"selector": {"$not": {"metadata_db": None}}, "fields": ["_id", "name"]},
    )
    r.raise_for_status()
    # print(r.json())
    valid_notebooks = []
    roles = token["_couchdb.roles"]
    notebook_json = r.json()["docs"]
    # pprint(roles)
    # pprint(notebook_json)
    # TODO Check against google and other invites instead of dc-managed-roles
    for notebook in notebook_json:
        if (
            "cluster-admin" in roles
            or f"{notebook['_id']}-admin" in roles
            or f"{notebook['_id']}||admin" in roles
        ):
            valid_notebooks.append({"notebook": notebook, "role": "admin"})
        elif notebook in roles:
            valid_notebooks.append({"notebook": notebook, "role": "user"})
    # pprint(valid_notebooks)
    return valid_notebooks


github_url_text.observe(validateURL, "value")


@out2.capture()
def prepare_select(notebook_list):
    options = []
    for notebook in notebook_list:
        options.append(
            (f"{notebook['notebook']['name']} ({notebook['role']})", notebook)
        )
    return options


@out2.capture()
def export_notebook(button):
    out2.clear_output()

    token = decode_token()
    notebook_id = notebook_select.value["notebook"]["_id"]
    server = token["base_url"]
    export_path_test = OUTPUT / f"{slugify(server)}+{notebook_id}"
    backup = export_path_test / "database_backup"

    zip_filename = f"{slugify(datetime.datetime.now().isoformat(timespec='minutes'))}+{notebook_id}+{slugify(server.replace('https',''))}.zip"
    tar_filename = f"{slugify(datetime.datetime.now().isoformat(timespec='minutes'))}+{notebook_id}+{slugify(server.replace('https',''))}.tgz"
    if export_path_test.exists():
        if overwrite_checkbox.value:
            shutil.rmtree(OUTPUT, ignore_errors=True)
            # shutil.rmtree(OUTPUT / zip_filename, ignore_errors=True)
        else:
            print("Output path exists. Please check 'overwrite' and try again!'")
            return
    print(f"Exporting notebook with id: {notebook_id} on {server}")

    export_csv(
        user=None,
        token=None,
        bearer_token=token["jwt_token"],
        base_url=server,
        project_key=notebook_id,
        inline_attachments=False,
        external_attachments=True,
    )

    backup.mkdir(parents=True)
    # Get readme, citation.cff, zipped repository, and replication streams for data and metadata
    metadata_doc = get_exporter_metadata()
    if "repository" in metadata_doc:
        g = Github()
        repo = g.get_repo(f"{metadata_doc['organisation']}/{metadata_doc['repo_name']}")
        try:
            readme = repo.get_contents("README.md")
            with open(export_path_test / "README.md", "wb") as readme_file:
                readme_file.write(base64.b64decode(readme.content))
        except Exception as e:
            print(f"Unable to save README.md. Reason: {e}")
        try:
            citation = repo.get_contents("CITATION.cff")
            with open(export_path_test / "CITATION.cff", "wb") as citation_file:
                citation_file.write(base64.b64decode(citation.content))
        except Exception as e:
            print(f"Unable to save CITATION.cff. Reason: {e}")

        # print(repo.master_branch)
        archive_url = repo.get_archive_link("tarball")
        # archive_url.format({'archive_format': "zip", })
        print(f"Downloading: {archive_url}")
        try:
            with requests.get(archive_url, stream=True) as archive_download:
                with open(
                    backup
                    / f"{metadata_doc['organisation']}-{metadata_doc['repo_name']}.zip",
                    "wb",
                ) as archive_file:
                    archive_download.raw.read = functools.partial(
                        archive_download.raw.read, decode_content=True
                    )

                    shutil.copyfileobj(archive_download.raw, archive_file)
        except Exception as e:
            print(f"Unable to download repository. Reason: {e}")

        def export_all_docs(token, db_prefix, notebook_id, filename):
            auth_token = BearerAuth(token["jwt_token"])
            url = f"{token['base_url']}/{db_prefix}-{notebook_id}/_all_docs"
            with requests.post(
                url,
                auth=auth_token,
                json={"include_docs": True, "attachments": True},
                stream=True,
            ) as response:
                response.raise_for_status()
                with open(filename, "wb") as json_file:
                    # json.dump(response.json(), json_file)
                    for chunk in tqdm(
                        response.iter_content(chunk_size=8192),
                        desc=f"Writing {notebook_id} {db_prefix} as json backup",
                    ):
                        json_file.write(chunk)

        export_all_docs(
            token,
            "metadata",
            notebook_id,
            backup / f"metadata_db-{notebook_id}.json",
        )
        export_all_docs(
            token, "data", notebook_id, backup / f"data_db-{notebook_id}.json"
        )

    if export_path_test.exists():
        # print("Zipping output/ directory")
        # with zipfile.ZipFile(
        #     OUTPUT / zip_filename,
        #     mode="w",
        #     compression=zipfile.ZIP_BZIP2,
        #     compresslevel=5,
        # ) as outputzip:
        #     with tqdm(
        #         export_path_test.glob("**/*"),
        #         desc="Preparing zip file",
        #     ) as iterator:
        #         for file in iterator:
        #             target_file = str(file).replace(
        #                 f"{OUTPUT / slugify(server)}",
        #                 f"{datetime.date.today().isoformat()}",
        #             )
        #             # if list_checkbox.value:
        #             #     iterator.write(target_file)

        #             outputzip.write(file, arcname=target_file)
        with tarfile.open(OUTPUT / tar_filename, "w:gz") as outputtar:
            with tqdm(
                export_path_test.glob("**/*"),
                desc="Preparing tar file",
                total=len(list(export_path_test.glob("**/*"))),
            ) as iterator:
                for file in iterator:
                    target_file = str(file).replace(
                        f"{OUTPUT / slugify(server)}",
                        f"{datetime.date.today().isoformat()}",
                    )
                    outputtar.add(file, arcname=target_file, recursive=False)
                    # if list_checkbox.value:
                    #     iterator.write(target_file)

    else:
        print("No records exported")
    display(HTML("<h2>Downloads</h2><ul>"))

    running_in_voila = os.environ.get("SERVER_SOFTWARE", "jupyter").startswith("voila")

    port_list = [note["port"] for note in notebookapp.list_running_servers()]

    if running_in_voila and os.environ["SERVER_PORT"] == "8866":
        files_path = ""
    else:
        files_path = "files/"
    # print(
    #     f"Debug for brian: {running_in_voila}, {port_list}, {files_path}, {os.environ.get('SERVER_PORT')}, {pformat([note for note in notebookapp.list_running_servers()])}"
    # )
    # print(running_in_voila and os.environ.get("SERVER_PORT") == "8866")

    for file in OUTPUT.glob("*.tgz"):
        local_url = HTML(
            f"<li><a href='{os.environ.get('VOILA_BASE_URL', '/')}{files_path}{file}'>Download export: {str(file).replace('output/','')}</li>"
        )
        # local_file = FileLink(file, result_html_prefix="Click here to download: ")
        display(local_url)
    display(HTML("</ul>"))


@out_url.capture()
def get_notebook_readme(change):
    out2.clear_output()
    github_url_text.value = ""
    # display(HTML())
    doc = get_exporter_metadata()
    # print(doc)

    if "repository" in doc:
        github_url_text.value = doc["repository"]


# list_notebooks()
notebook_select = widgets.Dropdown(
    options=["No notebooks loaded"],
    description="Choose notebook to export",
    style=desc_style,
    layout=Layout(width="60%", height="50px"),
)


overwrite_checkbox = widgets.Checkbox(
    value=True,
    description="Overwrite output directory when exporting",
    indent=False,
    style=desc_style,
)
list_checkbox = widgets.Checkbox(
    value=True, description="List files in export", indent=False, style=desc_style
)

export_button = widgets.Button(
    description="Export notebook",
    disabled=False,
    layout=Layout(width="20%", height="50px"),
    button_style="",  # 'success', 'info', 'warning', 'danger' or ''
    tooltip="Click here to connect to the server in order to validate the Bearer Token",
    icon="save",
)


def make_interface():
    display(show_button)
    display(bearer_token)
    display(validate_button)
    display(out)
    bearer_token.observe(check_for_valid, names=["value"])
    show_button.on_click(visible_bearer)
    validate_button.on_click(validate_database_connection)
    export_button.on_click(export_notebook)
