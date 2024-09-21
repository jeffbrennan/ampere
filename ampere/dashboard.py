import datetime
import random
import time
from dataclasses import dataclass
from functools import wraps

import duckdb
import networkx as nx
import pandas as pd
import plotly.graph_objects as go


def timeit(func):
    # https://dev.to/kcdchennai/python-decorator-to-measure-execution-time-54hk
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(f"Function {func.__name__} Took {total_time:.2f} seconds")
        return result

    return timeit_wrapper


@dataclass(slots=True, frozen=True)
class StargazerNetworkRecord:
    user_name: str
    followers_count: int
    starred_at: datetime.datetime
    retrieved_at: datetime.datetime
    repo_name: str


@timeit
def create_star_network(
    repos: list[str], stargazers: list[StargazerNetworkRecord]
) -> nx.Graph:
    random.seed(42)
    added_repos = []
    current_user = stargazers[0].user_name

    G = nx.Graph()
    for repo in repos:
        G.add_node(repo, node_type="repo", repo=repo)

    for record in stargazers:
        if record.user_name != current_user:
            added_repos = []
        node_name = f"{record.user_name}_{record.repo_name}"
        G.add_node(
            node_name,
            followers_count=record.followers_count,
            node_type="user_repo",
            repo=record.repo_name,
        )
        G.add_edge(node_name, record.repo_name, weight=50, edge_type="user_repo")
        if len(added_repos) > 0:
            for added_repo in added_repos:
                G.add_edge(
                    node_name,
                    f"{record.user_name}_{added_repo}",
                    edge_type="user_user",
                    weight=0.1,
                )
        added_repos.append(record.repo_name)
        current_user = record.user_name

    pos = nx.spring_layout(G)
    nx.set_node_attributes(G, pos, "pos")
    return G


@timeit
def create_star_network_plot(
    G: nx.Graph, repos: list[str], stargazers: list[StargazerNetworkRecord]
) -> go.Figure:
    edge_x = []
    edge_y = []
    for edge in G.edges():
        x0, y0 = G.nodes[edge[0]]["pos"]
        x1, y1 = G.nodes[edge[1]]["pos"]
        edge_x.append(x0)
        edge_x.append(x1)
        edge_x.append(None)
        edge_y.append(y0)
        edge_y.append(y1)
        edge_y.append(None)

    edge_trace = go.Scatter(
        x=edge_x,
        y=edge_y,
        line=dict(width=0.4, color="rgba(0, 0, 0, 0.2)"),
        hoverinfo="none",
        mode="lines",
        showlegend=False,
    )

    node_info = []
    for node in G.nodes():
        node_data = G.nodes.data()[node]
        repo = node_data["repo"]
        if node == repo:
            continue

        x, y = G.nodes[node]["pos"]
        followers_count = node_data["followers_count"]
        user_name = node.split("_")[0]

        all_repos = [i.repo_name for i in stargazers if i.user_name == user_name]

        all_repos_text = ", ".join(all_repos)

        node_text_list = [
            user_name,
            f"followers={followers_count}",
            f"repos={all_repos_text}",
        ]

        node_text = "<br>".join(node_text_list)
        node_info.append(
            {
                "x": x,
                "y": y,
                "repo": repo,
                "text": node_text,
                "size": followers_count,
            }
        )

    node_df = pd.DataFrame(node_info)
    node_df["size_group"] = pd.qcut(node_df["size"], 6, labels=False, duplicates="drop")
    node_df["size_group"] = (node_df["size_group"] + 1) * 5

    all_node_traces = []
    for repo in repos:
        repo_df = node_df.query(f"repo == '{repo}'")
        node_trace = go.Scatter(
            x=repo_df.x,
            y=repo_df.y,
            marker_size=repo_df.size_group,
            mode="markers",
            hoverinfo="text",
            hovertext=repo_df.text,
            name=repo,
        )
        all_node_traces.append(node_trace)

    last_updated = max([i.retrieved_at for i in stargazers])
    last_updated_str = last_updated.strftime("%Y-%m-%d")

    title_text = f"mrpowers-io Stargazers<br><sup>last updated: {last_updated_str}</sup>"
    fig = go.Figure(
        data=[edge_trace, *all_node_traces],
        layout=go.Layout(
            title=title_text,
            showlegend=True,
            hovermode="closest",
            margin=dict(b=20, l=5, r=5, t=55),
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        ),
    )

    return fig


@timeit
def viz_star_network():
    con = duckdb.connect("../data/ampere.duckdb")
    stargazers = con.sql(
        """
        select
        distinct
        a.user_name,
        a.followers_count,
        b.starred_at,
        b.retrieved_at,
        c.repo_name
        from users a 
        inner join stargazers b
        on a.user_id = b.user_id
        inner join repos c
        on b.repo_id = c.repo_id
        order by user_name 
        """
    ).to_df()

    stargazers = list(StargazerNetworkRecord(*record) for record in stargazers.values)  # pyright: ignore [reportAttributeAccessIssue]
    repos = sorted(set(i.repo_name for i in stargazers))

    network = create_star_network(repos, stargazers)
    fig = create_star_network_plot(network, repos, stargazers)
    fig.show()


if __name__ == "__main__":
    viz_star_network()
