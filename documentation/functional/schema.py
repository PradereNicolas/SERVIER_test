"""Functionnal schema automated documentation
"""

import os
from pathlib import Path
from dataclasses import dataclass
import importlib

import networkx as nx
import matplotlib.pyplot as plt
import matplotlib

from areas import AreaType
from areas.area import DataFormat

ROOT_DIR = "areas"
JOBS_DIR = "jobs"
DATA_DIR = "data"

OUTPUT_FILE = "schema.png"
CURRENT_PATH = os.path.dirname(os.path.realpath(__file__))

JOB_NODE_TYPE = "JOB"
DATA_NODE_TYPE = "DATA"

AREA_COLORS = {
    AreaType.RAW: [1, 0, 0],
    AreaType.REFINED: [1, 0.5, 0],
    AreaType.OPTIMIZED: [1, 1, 0],
    AreaType.BUSINESS: [0, 1, 0],
}

DATA_NODES_LAYOUT = {area: 2*index+1 for index, area in enumerate(AreaType)}
JOBS_NODES_LAYOUT = {area: 2*index for index, area in enumerate(AreaType)}


def generate_data_source_id(area: AreaType, data_type: str) -> str:
    """Generate a data source node id

    Args:
        area (AreaType): Data source area
        data_type (str): Data source name

    Returns:
        str: Id of the data node
    """
    return f"{area.name}_{data_type.upper()}"

@dataclass
class DataNode:
    """Data node data class
    """
    data_format: DataFormat
    area: AreaType
    name: str

    @property
    def id_(self) -> str:
        """Id of the data node

        Returns:
            str: Id of the node
        """
        return generate_data_source_id(area=self.area, data_type=self.name)

@dataclass
class JobNode:
    """Job node data class
    """
    area: AreaType
    name: str
    sources_ids: list[str]
    targets_id: list[str]

    @property
    def id_(self) -> str:
        """Id of the job node

        Returns:
            str: Id of the node
        """
        return f"JOB_{generate_data_source_id(area=self.area, data_type=self.name)}"

graph_figure = nx.DiGraph()

def get_data_nodes() -> list[DataNode]:
    """Get all data nodes

    Returns:
        list[DataNode]: List of data nodes
    """
    def get_raw_area_data_sources() -> list[DataNode]:
        """Specific handling of raw area

        Returns:
            list[DataNode]: Raw data nodes
        """
        raw_data_sources = []
        raw_data_directory = f"{ROOT_DIR}/{AreaType.RAW.value}/{DATA_DIR}"
        data_sources = os.listdir(raw_data_directory)
        for data_source in data_sources:
            data_formats = os.listdir(f"{raw_data_directory}/{data_source}")
            raw_data_sources.extend([DataNode(
                data_format=DataFormat(data_format),
                area=AreaType.RAW,
                name=data_source
            ) for data_format in data_formats])
        return sorted(raw_data_sources, key=lambda node: node.name)

    def get_area_data_sources(area: AreaType) -> list[DataNode]:
        """Get data nodes for area

        Args:
            area (AreaType): Area to be sniffed

        Returns:
            list[DataNode]: Area data nodes
        """
        def get_data_sources_from_folder(
            folder_path: Path,
            area: AreaType) -> list[DataNode]:
            """Return list of data nodes in a data folder

            Args:
                folder_path (Path): Data folder path
                area (AreaType): Area

            Returns:
                list[DataNode]: List of data nodes
            """
            folder_data_nodes = []
            data_files = os.listdir(folder_path)
            for data_file in data_files:
                name, data_format = data_file.split(".")
                folder_data_nodes.append(DataNode(
                    name=name,
                    data_format=DataFormat(data_format),
                    area=area
                ))
            return folder_data_nodes

        area_data_sources = []
        data_directory = f"{ROOT_DIR}/{area.value}/{DATA_DIR}"
        data_sources = os.listdir(data_directory)
        for data_source in data_sources:
            area_data_sources.extend(get_data_sources_from_folder(
                folder_path=f"{data_directory}/{data_source}",
                area=area
            ))
        return sorted(area_data_sources, key=lambda node: node.name)

    all_data_nodes = []
    for area in AreaType:
        if area == AreaType.RAW:
            all_data_nodes.extend(get_raw_area_data_sources())
            continue
        all_data_nodes.extend(get_area_data_sources(area=area))
    return all_data_nodes

def get_jobs_nodes() -> list[JobNode]:
    """Retrive all jobs nodes

    Returns:
        list[JobNode]: Jobs nodes
    """
    all_jobs_nodes = []
    for area in AreaType:
        area_path = f"{ROOT_DIR}/{area.value}"
        directories = os.listdir(area_path)
        if JOBS_DIR not in directories:
            continue
        jobs_path = f"{area_path}/{JOBS_DIR}"
        python_jobs_path = jobs_path.replace("/", ".")
        jobs_names = os.listdir(jobs_path)
        for job_name in jobs_names:
            python_job_name = job_name.split(".")[0]
            job_module = importlib.import_module(f"{python_jobs_path}.{python_job_name}")
            try:
                job = job_module.Job()
            except AttributeError:
                continue
            target_data_type = job.target_data_type.value
            target_data_id = generate_data_source_id(
                area=area,
                data_type=target_data_type
            )
            target_reject_data_id = f"{target_data_id}_REJECTED"
            all_jobs_nodes.append(JobNode(
                area=area,
                name=python_job_name,
                sources_ids=[generate_data_source_id(
                    area=source.area,
                    data_type=source.data_type.value
                ) for source in job.sources],
                targets_id=[target_data_id, target_reject_data_id]
            ))
    return all_jobs_nodes


data_nodes = get_data_nodes()
jobs_nodes = get_jobs_nodes()

all_nodes = [*data_nodes, *jobs_nodes]

graph_figure = nx.DiGraph()

for index, node in enumerate(data_nodes):
    graph_figure.add_node(
        node.id_,
        name=node.name,
        type=DATA_NODE_TYPE,
        color=tuple(color for color in [*AREA_COLORS[node.area], 1]),
        x_position=DATA_NODES_LAYOUT[node.area],
        area=node.area)
for index, node in enumerate(jobs_nodes):
    graph_figure.add_node(
        node.id_,
        name=node.name,
        type=JOB_NODE_TYPE,
        color=tuple(color for color in [*AREA_COLORS[node.area], 0.3]),
        x_position=JOBS_NODES_LAYOUT[node.area],
        area=node.area)
    for source_id in node.sources_ids:
        graph_figure.add_edge(source_id, node.id_)
    for target_id in node.targets_id:
        graph_figure.add_edge(node.id_, target_id)

plt.ioff()
matplotlib.use("Agg")

plt.figure(figsize=(16, 12))

labels = nx.get_node_attributes(graph_figure, "name")

data_area_nodes_count = {
    area.value: sum([1 for _, data in graph_figure.nodes(data=True) \
        if data["area"] == area.value] and data["type"] == DATA_NODE_TYPE) \
        for area in AreaType}

jobs_area_nodes_count = {
    area.value: sum([1 for _, data in graph_figure.nodes(data=True) \
        if data["area"] == area.value] and data["type"] == JOB_NODE_TYPE) \
        for area in AreaType}

pos = nx.spring_layout(graph_figure)

previous_layer = None
previous_type = None
layer_counter = 0

for node, data in graph_figure.nodes(data=True):
    if previous_layer is None or data["area"] != previous_layer:
        layer_counter = 0
        previous_layer = data["area"]
    if previous_type is None or data["type"] != previous_type:
        layer_counter = 0
        previous_type = data["type"]
    pos[node] = (data["x_position"], layer_counter)
    layer_counter += 1

nx.draw(
    graph_figure,
    pos,
    # with_labels=False,
    node_size=3_000,
    edge_color="gray",
    font_size=10,
    node_color=[graph_figure.nodes[node].get("color") for node in graph_figure.nodes])

nx.draw_networkx_labels(graph_figure, pos, labels, font_size=10)

plt.savefig(f"{CURRENT_PATH}/{OUTPUT_FILE}", format="png", dpi=300)

plt.show()
