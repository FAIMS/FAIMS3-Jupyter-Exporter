from pprint import pprint, pformat


def hook_function(req, notebook, cwd):
    """Do your stuffs here"""
    print("Hi Brian")
    pprint(req)
    pprint(notebook)
    pprint(cwd)
    return notebook


c.Voila.prelaunch_hook = hook_function
