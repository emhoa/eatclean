import os
import tornado.httpserver
import tornado.ioloop
from tornado.web import FallbackHandler, RequestHandler, Application 
from tornado.wsgi import WSGIContainer
from app import app
 
class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hi world")
 
def main():
	tr = WSGIContainer(app)

	application = tornado.web.Application([
        	(r"/", MainHandler),
		(r".*", FallbackHandler, dict(fallback=tr)),
	    ])
	http_server = tornado.httpserver.HTTPServer(application)
	port = int(os.environ.get("PORT", 5000))
	http_server.listen(port)
	tornado.ioloop.IOLoop.instance().start()
 
if __name__ == "__main__":
    main()
