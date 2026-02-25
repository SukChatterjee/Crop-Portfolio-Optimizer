from __future__ import annotations

from .nodes import fetch_and_compute, finalize, llm_enrich, validate_inputs
from .state import AgentState


def _route_after_validate(state: AgentState) -> str:
    if state.get("errors"):
        return "finalize"
    return "fetch_and_compute"


class _FallbackGraph:
    def invoke(self, state: AgentState):
        current = dict(state)
        current.update(validate_inputs(current))
        if _route_after_validate(current) == "fetch_and_compute":
            current.update(fetch_and_compute(current))
            current.update(llm_enrich(current))
        current.update(finalize(current))
        return current


def build_graph():
    try:
        from langgraph.graph import END, StateGraph
    except ImportError:
        return _FallbackGraph()

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
