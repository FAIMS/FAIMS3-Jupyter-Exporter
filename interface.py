import ipywidgets as widgets
from ipywidgets import Button, Layout
import jwt
import base64
import json
from pprint import pprint


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
import tqdm
import zipfile
from IPython.display import FileLink, HTML

OUTPUT = Path("output")
FORMAT = (
    "[%(asctime)s %(filename)s->%(funcName)s():%(lineno)s]%(levelname)s: %(message)s"
)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

logging.basicConfig(format=FORMAT, level=logging.WARNING)

out = widgets.Output(layout={"border": "0px solid black"})
out2 = widgets.Output(layout={"border": "0px solid black"})

layout_hidden = widgets.Layout(display="none")
layout_visible = widgets.Layout(display="block")

desc_style = {"description_width": "initial"}

bearer_token = widgets.Text(
    value="",
    placeholder="",
    description="Bearer Token:",
    disabled=False,
    style=desc_style,
)


show_button = widgets.Button(
    description="Show bearer token text field", layout=layout_hidden
)


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
    print("Validating...")
    try:
        token = decode_token()
        # print(token)
        # jwt_token = jwt.decode(token['jwt_token'], token['public_key'], algorithms=[token['alg']])
        print(f"Hello: {token['name']} ({token['sub']}) on {token['base_url']}")

        # out.append_stdout(token)

        bearer_token.layout = layout_hidden
        show_button.layout = layout_visible
        notebook_select.values=prepare_select(list_notebooks())
        
        display(notebook_select)
        display(overwrite_checkbox)
        display(list_checkbox)
        display(export_button)
        # list_notebooks()
        display(out2)
    except:
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
        if "cluster-admin" in roles or f"{notebook['_id']}-admin" in roles:
            valid_notebooks.append({"notebook": notebook, "role": "admin"})
        elif notebook in roles:
            valid_notebooks.append({"notebook": notebook, "role": "user"})
    # pprint(valid_notebooks)
    return valid_notebooks


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
    zip_filename = f"{slugify(datetime.datetime.now().isoformat(timespec='minutes'))}+{notebook_id}+{slugify(server.replace('https',''))}.zip"
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
    if export_path_test.exists():
        print("Zipping output/ directory")
        with zipfile.ZipFile(
            OUTPUT / zip_filename,
            mode="w",
            compression=zipfile.ZIP_BZIP2,
            compresslevel=5,
        ) as outputzip:
            with tqdm.tqdm(export_path_test.glob("**/*")) as iterator:
                for file in iterator:
                    target_file = str(file).replace(
                        f"{OUTPUT / slugify(server)}",
                        f"{datetime.date.today().isoformat()}",
                    )
                    if list_checkbox.value:
                        iterator.write(target_file)

                    outputzip.write(file, arcname=target_file)
    else:
        print("No records exported")
    display(HTML("<h2>Downloads</h2><ul>"))
    for file in OUTPUT.glob("*.zip"):
        local_url = HTML(
            f"<li><a download href='/files/{file}'>Download export: {str(file).replace('output/','')}</li>"
        )
        # local_file = FileLink(file, result_html_prefix="Click here to download: ")
        display(local_url)
    display(HTML("</ul>"))


# list_notebooks()
notebook_select = widgets.Dropdown(
    options=[],
    description="Choose notebook to export",
    style=desc_style,
    layout=Layout(width="60%", height="50px"),
)

overwrite_checkbox = widgets.Checkbox(
    value=False,
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
