from django.http import HttpResponse
from django_ratelimit.decorators import ratelimit
from django_ratelimit.exceptions import Ratelimited
from functools import wraps


class GlobalRateLimitMiddleware:
    """
    Middleware para aplicar rate limiting global de 50 requests por minuto
    basado en la IP del cliente.
    """
    
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        # Aplicar rate limit de 50 requests por minuto por IP
        @ratelimit(key='ip', rate='50/m', method='ALL')
        def rate_limited_view(request):
            return self.get_response(request)
        
        try:
            return rate_limited_view(request)
        except Ratelimited:
            # Respuesta personalizada cuando se excede el rate limit
            return HttpResponse(
                "Demasiadas solicitudes. Por favor, espera un momento antes de volver a intentar.",
                status=429,
                content_type='text/plain; charset=utf-8'
            )

    def process_exception(self, request, exception):
        """Maneja las excepciones de rate limiting"""
        if isinstance(exception, Ratelimited):
            return HttpResponse(
                "Demasiadas solicitudes. Por favor, espera un momento antes de volver a intentar.",
                status=429,
                content_type='text/plain; charset=utf-8'
            )
        return None
