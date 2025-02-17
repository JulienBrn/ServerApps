import panel as pn
from pathlib import Path
import yaml, sys

current_location = Path(".").resolve()

to_rm =[]
files = {str(f.relative_to(current_location)):f for f in Path(current_location).glob("**/*.md")} | {str(f.relative_to(current_location)):f for f in Path(current_location).glob("**/*.pdf")}
for f in files:
    for p in files[f].parts:
        if p.startswith("."):
            to_rm.append(f)
            
files = {f:v for f,v in files.items() if not f in to_rm}

select = pn.widgets.Select(name='Select', options=list(files.keys()))

display = pn.pane.Placeholder()

def update_display(v):
    f = files[v]
    if f.suffix ==".pdf":
        display.update(pn.pane.PDF(f, sizing_mode='stretch_width', height=1000))
    elif f.suffix == ".md":
        with f.open("r") as fi:
            display.update(pn.pane.Markdown(fi.read()))
    else:
        raise Exception(f"Unknwon suffix {f.suffix}")

update_display(select.value)
pn.bind(update_display, select, watch=True)


pn.Column(select, display, width_policy='max').servable()
