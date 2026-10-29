"""Local protocol fixture; never used by production AI functions."""
import argparse
import hashlib
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

lock = threading.Lock()
requests = []

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def respond(self, data, status=200):
        payload = json.dumps(data, ensure_ascii=False).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        if self.path == '/metrics':
            with lock:
                self.respond({'requests': list(requests)})
        else:
            self.respond({'error': 'not found'}, 404)

    def do_POST(self):
        try:
            length = int(self.headers.get('Content-Length', 0))
            if length <= 0 or length > 16 * 1024 * 1024:
                self.respond({'error': 'invalid request size'}, 413)
                return
            body = json.loads(self.rfile.read(length))
            with lock:
                requests.append({'path': self.path, 'input_count': len(body['input']) if isinstance(body.get('input'), list) else 1})
            faults = {
                '/fault/empty_completion': {'choices': [{'message': {'content': ''}}]},
                '/fault/wrong_completion_type': {'choices': [{'message': {'content': {}}}]},
                '/fault/duplicate_embedding': {'data': [{'index': 0, 'embedding': [1,2,3]}, {'index': 0, 'embedding': [4,5,6]}]},
                '/fault/bad_embedding_values': {'data': [{'index': 0, 'embedding': ['bad', None, 1]}]},
                '/fault/huge_embedding_values': {'data': [{'index': 0, 'embedding': [1e100, 2, 3]}]},
                '/fault/wrong_dimension': {'data': [{'index': 0, 'embedding': [1]}]},
                '/fault/bad_rerank': {'results': [{'index': 99, 'relevance_score': 1}]},
                '/fault/http_error': {'error': 'injected service failure'},
            }
            if self.path in faults:
                self.respond(faults[self.path], 500 if self.path == '/fault/http_error' else 200)
                return
            if self.path == '/v1/chat/completions':
                content = body['messages'][-1]['content']
                self.respond({'choices': [{'message': {'content': 'mock:' + content}}]})
            elif self.path == '/api/generate':
                if body.get('stream') is not False:
                    self.respond({'error': 'stream must be disabled'}, 400)
                    return
                self.respond({'response': 'mock:' + body['prompt'], 'done': True})
            elif self.path in ('/v1/embeddings', '/api/embed'):
                inputs = body['input']
                if isinstance(inputs, str):
                    inputs = [inputs]
                dimension = body.get('dimensions', 3)
                if not isinstance(dimension, int) or not 0 < dimension <= 4096:
                    self.respond({'error': 'invalid dimensions'}, 400)
                    return
                vectors = []
                for text in inputs:
                    seed = hashlib.sha256(text.encode('utf-8')).digest()
                    vectors.append([(seed[i % len(seed)] - 128) / 128.0 for i in range(dimension)])
                if self.path == '/api/embed':
                    self.respond({'embeddings': vectors})
                else:
                    self.respond({'data': list(reversed([{'index': i, 'embedding': v} for i, v in enumerate(vectors)]))})
            elif self.path == '/v1/rerank':
                documents = body['documents']
                scores = [{'index': i, 'relevance_score': len(text) / 100.0} for i, text in enumerate(documents)]
                self.respond({'results': list(reversed(scores))})
            else:
                self.respond({'error': 'not found'}, 404)
        except (ValueError, TypeError, KeyError) as exc:
            self.respond({'error': str(exc)}, 400)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=8000)
    args = parser.parse_args()
    ThreadingHTTPServer((args.host, args.port), Handler).serve_forever()
