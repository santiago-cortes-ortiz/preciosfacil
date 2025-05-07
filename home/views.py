from django.shortcuts import render

from .service import process_search

# Create your views here.
def home(request):
    if request.method == 'POST':
        search_query = request.POST.get('search_item')
        process_search(search_query)
        print(f"Value of search_query: {search_query}")
    return render(request, 'home.html')