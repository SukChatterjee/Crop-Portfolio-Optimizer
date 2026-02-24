from __future__ import annotations

from langgraph.graph import END, StateGraph

from .nodes import fetch_and_compute, finalize, llm_enrich, validate_inputs
from .state import AgentState


def _route_after_validate(state: AgentState) -> str:
    if state.get("errors"):
        return "finalize"
    return "fetch_and_compute"


def build_graph():
    graph = StateGraph(AgentState)
    graph.add_node("validate_inputs", validate_inputs)
    graph.add_node("fetch_and_compute", fetch_and_compute)
    graph.add_node("llm_enrich", llm_enrich)
    graph.add_node("finalize", finalize)

    graph.set_entry_point("validate_inputs")
    graph.add_conditional_edges(
        "validate_inputs",
        _route_after_validate,
        {
            "fetch_and_compute": "fetch_and_compute",
            "finalize": "finalize",
        },
    )
    graph.add_edge("fetch_and_compute", "llm_enrich")
    graph.add_edge("llm_enrich", "finalize")
    graph.add_edge("finalize", END)

    return graph.compile()

