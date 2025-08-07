from django.shortcuts import render

from .service import search_aggregated


def home(request):
    search_query = ""
    results_data = {"results": [], "errors": []}

    if request.method == "POST":
        search_query = request.POST.get("search_item", "").strip()
        selected_sources = request.POST.getlist("sources") or ["mercadolibre"]
        results_data = search_aggregated(search_query, sources=selected_sources, max_items_per_source=5)

    context = {
        "search_query": search_query,
        "results": results_data.get("results", []),
        "errors": results_data.get("errors", []),
        "selected_sources": results_data.get("sources", []),
    }
    return render(request, "home.html", context)