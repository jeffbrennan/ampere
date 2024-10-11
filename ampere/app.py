import datetime
import dash
import dash_bootstrap_components as dbc

from ampere.common import get_db_con
from dash import dcc, html

app = dash.Dash(
    __name__,
    use_pages=True,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
)


def get_last_updated() -> datetime.datetime:
    con = get_db_con()
    max_retrieved_at = (
        con.sql("SELECT max(retrieved_at) as last_updated FROM main.repos")
        .to_df()
        .squeeze()
    )

    return max_retrieved_at


last_updated = get_last_updated()
last_updated_str = last_updated.strftime("%Y-%m-%d")

navbar = dbc.NavbarSimple(
    dbc.Nav(
        [
            dbc.NavLink(page["name"], href=page["path"])
            for page in dash.page_registry.values()
            if page.get("top_nav")
        ],
    ),
    brand="ampere",
    color="#3F6DF9",
    className="mb-2",
    dark=True,
    style={"width": "100%", "color": "#FFFFFF"},
)

footer = html.Footer(
    children=[html.Div(f"last updated {last_updated_str}", style={"textAlign": "left"})],
    style={
        "position": "fixed",
        "bottom": "0",
        "width": "100%",
        "backgroundColor": "#3F6DF9",
        "color": "#FFFFFF",
        "paddingLeft": "4.5vw",
    },
)
app.layout = dbc.Container(
    [navbar, dash.page_container, footer],
    fluid=True,
)

if __name__ == "__main__":
    app.run_server(debug=True)
