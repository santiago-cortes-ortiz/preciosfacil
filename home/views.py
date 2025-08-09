from django.shortcuts import render

from .service import search_aggregated, get_available_sources


def home(request):
    search_query = ""
    results_data = {"results": [], "errors": []}
    available_sources = get_available_sources()

    if request.method == "POST":
        search_query = request.POST.get("search_item", "").strip()
        selected_sources = request.POST.getlist("sources") or [s["key"] for s in available_sources]
        results_data = search_aggregated(search_query, sources=selected_sources, max_items_per_source=5)

    context = {
        "search_query": search_query,
        "results": results_data.get("results", []),
        "errors": results_data.get("errors", []),
        "selected_sources": results_data.get("sources", []),
        "best_item": results_data.get("best_item"),
        "available_sources": available_sources,
    }
    return render(request, "home.html", context)