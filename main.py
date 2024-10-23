"""Main pipelines module
"""

from enum import Enum
import json

from dataclasses import dataclass

import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
import matplotlib

from areas import REFINED_AREA, OPTIMIZED_AREA, BUSINESS_AREA, process_data

# WORKFLOWS
process_data(
    refined_area=REFINED_AREA,
    optimized_area=OPTIMIZED_AREA,
    business_area=BUSINESS_AREA)

# FINAL TEST RESULT
class NodeType(Enum):
    """Possible type of nodes
    """
    CLINICAL_TRIAL = "CLINICAL_TRIAL"
    PUBMED = "PUBMED"
    JOURNAL = "JOURNAL"
    DRUG = "DRUG"

NODE_TYPE_TABLE_ID_COLUMNS_MAPPING = {
    NodeType.CLINICAL_TRIAL: "publication_id",
    NodeType.PUBMED: "publication_id",
    NodeType.JOURNAL: "journal_id",
    NodeType.DRUG: "drug_id"
}

NODE_TYPE_TABLE_VALUES_COLUMNS_MAPPING = {
    NodeType.CLINICAL_TRIAL: "functional_id",
    NodeType.PUBMED: "functional_id",
    NodeType.JOURNAL: "journal_name",
    NodeType.DRUG: "drug"
}

NODE_DEPENDENCIES = {
    NodeType.CLINICAL_TRIAL: [NodeType.DRUG],
    NodeType.PUBMED: [NodeType.DRUG],
    NodeType.JOURNAL: [NodeType.CLINICAL_TRIAL, NodeType.PUBMED],
    NodeType.DRUG: [NodeType.JOURNAL]
}

class Node:
    """Graph node class
    """

    @dataclass
    class ParentReference:
        """Parent Node data class
        """
        id_: str
        type_: NodeType
        date: str = None

        def __hash__(self) -> str:
            """Return hash

            Returns:
                str: Hash of reference
            """
            return hash((self.type_, self.id_))

        def __eq__(self, other) -> bool:
            """Return if other is equal to current one

            Returns:
                bool: Boolean value indicating if other is equal to current one
            """
            return self.type_ == other.type_ and self.id_ == other.id_

    def __init__(self, type_: NodeType, value: str, id_: str) -> None:
        """Node initialiser

        Args:
            type_ (NodeType): Type of the node
            value (str): Value of the node
            id_ (str): ID od the node
        """
        self.type_ = type_
        self.value = value
        self.id_ = id_
        self.dependencies_type = NODE_DEPENDENCIES[self.type_]
        self.parents: set[self.ParentReference] = set()

    def to_dict(self) -> dict:
        """Dict values to be written as json

        Returns:
            dict: Dict value
        """
        return {
            "id": self.id_,
            "type": self.type_.name,
            "value": self.value,
            "parents": [{
                "id": parent.id_,
                "type": parent.type_.name,
                "date": parent.date
                } for parent in self.parents]
        }

    def add_parent(self, row: pd.Series) -> None:
        """Add a parent

        Args:
            row (pd.Series): Mentions dataframe record
        """
        dependency_type = self.dependencies_type[0] if len(self.dependencies_type) == 1 \
            else NodeType[row.publication_type]

        new_reference = self.ParentReference(
            id_=row[NODE_TYPE_TABLE_ID_COLUMNS_MAPPING[dependency_type]],
            type_=dependency_type
        )
        if self.type_ == NodeType.JOURNAL:
            new_reference.date = row.publication_date
        if self.type_ == NodeType.DRUG and dependency_type == NodeType.JOURNAL and \
            row.publication_type == NodeType.PUBMED.name:
            new_reference.date = row.publication_date

        self.parents.add(new_reference)

class Graph:
    """Final graph class
    """
    def __init__(self) -> None:
        """Graph initialiser
        """
        self.nodes: list[Node] = []
        self.passed_nodes: dict[set] = {
            type_: set() for type_ in NodeType
        }

    def write_flat_json(self, path="flat_result.json") -> None:
        """Write flat json

        Args:
            path (str, optional): Path. Defaults to "result.json".
        """
        with open(path, "w", encoding="latin-1") as file:
            json.dump([node.to_dict() for node in self.nodes], file)

    def save_graph_figure(self, path="graph.png") -> None:
        """Save PNG file with the figure

        Args:
            path (str, optional): Path. Defaults to "graph.png".
        """
        graph_figure = nx.DiGraph()

        visited = set()
        def add_node_with_parents(node: Node, visited, depth=0, max_depth=1_000):
            parents = node.parents
            if node.id_ in visited:
                return
            if depth > max_depth:
                return
            visited.add(node.id_)

            graph_figure.add_node(node.id_, label=node.value, type=node.type_)

            for parent in parents:
                graph_figure.add_edge(node.id_, parent.id_)
                parent_node = next((node for node in self.nodes if node.id_ == parent.id_), None)
                if parent_node:
                    add_node_with_parents(
                        node=parent_node,
                        visited=visited,
                        depth=depth + 1,
                        max_depth=max_depth)

        for node in self.nodes:
            add_node_with_parents(node=node, visited=visited)

        plt.ioff()
        matplotlib.use("Agg")

        plt.figure(figsize=(16, 12))

        labels = nx.get_node_attributes(graph_figure, "label")

        pos = nx.spring_layout(graph_figure, seed=42)
        nx.draw(
            graph_figure,
            pos,
            with_labels=False,
            node_size=3_000,
            node_color="skyblue",
            edge_color="gray",
            font_size=10)

        nx.draw_networkx_labels(graph_figure, pos, labels, font_size=10)

        plt.savefig(path, format="png", dpi=300)

        plt.show()

    def check_node_existence(self, node: Node) -> bool:
        """Returns wether or not the node already is in the graph

        Args:
            node (Node): Node to be checked

        Returns:
            bool: Boolean value indicating if the node exists in the graph
        """
        passed_nodes = self.passed_nodes[node.type_] if node.type_ not in [NodeType.CLINICAL_TRIAL, NodeType.PUBMED] \
            else self.passed_nodes[NodeType.CLINICAL_TRIAL].union(self.passed_nodes[NodeType.PUBMED])
        return node.id_ in passed_nodes

    def add_node(self, node: Node) -> None:
        """Add a new node

        Args:
            node (Node): Node to be added
        """
        self.nodes.append(node)
        self.passed_nodes[node.type_].add(node.id_)

    def process_node(self, node: Node, row: pd.Series) -> None:
        """Process a new node

        Args:
            node (Node): New node
            row (pd.Series): Current row
        """
        if self.check_node_existence(node=node) is False:
            self.add_node(node=node)
            return
        for node_ in self.nodes:
            if node_.id_ == node.id_:
                node_.add_parent(row=row)
                return

    def process_dataframe_row(self, row: pd.Series) -> None:
        """Process a data frame row

        Args:
            row (pd.Series): Dataframe row
        """
        for node_type in NodeType:
            self.process_node(
                node=Node(
                    type_=node_type,
                    value=row[NODE_TYPE_TABLE_VALUES_COLUMNS_MAPPING[node_type]],
                    id_=row[NODE_TYPE_TABLE_ID_COLUMNS_MAPPING[node_type]]
                ),
                row=row
            )

mentions_df = pd.read_pickle("areas/business/data/mention/mention.pkl")
graph = Graph()

for _, row_ in mentions_df.iterrows():
    graph.process_dataframe_row(row=row_)

graph.write_flat_json()
graph.save_graph_figure()
