from datetime import datetime 

class TimingMiddleware:
    def __init__(self, app):
        self.app = app

    def __call__(self, env, start_response):
        start_time = datetime.now()

        def injecting_start_response(status, headers, exc_info=None):
            end_time = datetime.now()
            time_taken = (end_time - start_time).total_seconds()
            headers.append(('X-Time-Taken', str(time_taken)))
            return start_response(status, headers, exc_info)

        return self.app(env, injecting_start_response)