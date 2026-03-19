from __future__ import annotations

from .nodes import (
    agent2_predict,
    compute_results,
    fetch_source_data,
    finalize,
    llm_enrich,
    plan_sources,
    validate_inputs,
)
from .state import AgentState


def _route_after_validate(state: AgentState) -> str:
    if state.get("errors"):
        return "finalize"
    return "plan_sources"


class _FallbackGraph:
    def invoke(self, state: AgentState):
        current = dict(state)
        current.update(validate_inputs(current))
        if _route_after_validate(current) == "plan_sources":
            current.update(plan_sources(current))
            current.update(fetch_source_data(current))
            current.update(agent2_predict(current))
            current.update(compute_results(current))
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
    graph.add_node("plan_sources", plan_sources)
    graph.add_node("fetch_source_data", fetch_source_data)
    graph.add_node("agent2_predict", agent2_predict)
    graph.add_node("compute_results", compute_results)
    graph.add_node("llm_enrich", llm_enrich)
    graph.add_node("finalize", finalize)

    graph.set_entry_point("validate_inputs")
    graph.add_conditional_edges(
        "validate_inputs",
        _route_after_validate,
        {
            "plan_sources": "plan_sources",
            "finalize": "finalize",
        },
    )
    graph.add_edge("plan_sources", "fetch_source_data")
    graph.add_edge("fetch_source_data", "agent2_predict")
    graph.add_edge("agent2_predict", "compute_results")
    graph.add_edge("compute_results", "llm_enrich")
    graph.add_edge("llm_enrich", "finalize")
    graph.add_edge("finalize", END)

    return graph.compile()
