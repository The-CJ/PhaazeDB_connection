import requests, json

class Connection():
	def __init__(self, adress="http://127.0.0.1", port="", token=None, exception_on_error=False):
		self.session = requests.session()
		self.adress = adress
		self.port = str(port) if port == "" else ":" + str(port)
		self.token = token
		self.exception_on_error = exception_on_error

	def set_connection(self, adress="http://127.0.0.1", port="", token=None):
		self.adress = adress
		self.port = str(port) if port == "" else ":" + str(port)
		self.token = token

	def create(self, name=None):
		if name == None: raise AttributeError("'name' can't be None")

		call = dict(
			action="create",
			token=self.token,
			name=name)

		try: r = self.session.post(self.adress+self.port, json=call)
		except: raise ConnectionError("Failed to connect")

		res = json.loads(r.text)
		if res.get('status', 'error') == 'error' and self.exception_on_error:
			raise Connection.ErrorFromDB("PhaazeDB returned: "+str(res))

		return res

	def drop(self, name=None):
		if name == None: raise AttributeError("'name' can't be None")

		call = dict(
			action="drop",
			token=self.token,
			name=name)

		try: r = self.session.post(self.adress+self.port, json=call)
		except: raise ConnectionError("Failed to connect")

		res = json.loads(r.text)
		if res.get('status', 'error') == 'error' and self.exception_on_error:
			raise Connection.ErrorFromDB("PhaazeDB returned: "+str(res))

		return res

	def insert(self, into=None, content=None):
		if into == None or content == None: raise AttributeError("'into' or 'content' can't be None")

		if type(content) is not dict: raise AttributeError("'content' must be dict")

		call = dict(
			action="insert",
			token=self.token,
			into=into,
			content=content)

		try: r = self.session.post(self.adress+self.port, json=call)
		except: raise ConnectionError("Failed to connect")

		res = json.loads(r.text)
		if res.get('status', 'error') == 'error' and self.exception_on_error:
			raise Connection.ErrorFromDB("PhaazeDB returned: "+str(res))

		return res

	def delete(self, of=None, where=""):
		if of == None: raise AttributeError("'of' can't be None")

		call = dict(
			action="delete",
			token=self.token,
			of=of,
			where=where)

		try: r = self.session.post(self.adress+self.port, json=call)
		except: raise ConnectionError("Failed to connect")

		res = json.loads(r.text)
		if res.get('status', 'error') == 'error' and self.exception_on_error:
			raise Connection.ErrorFromDB("PhaazeDB returned: "+str(res))

		return res

	def update(self, of=None, where="", content=None):
		if of == None or content == None: raise AttributeError("'of' can't be None")

		if type(content) is not dict: raise AttributeError("'content' must be dict")

		call = dict(
			action="update",
			token=self.token,
			of=of,
			where=where,
			content=content
			)

		try: r = self.session.post(self.adress+self.port, json=call)
		except: raise ConnectionError("Failed to connect")

		res = json.loads(r.text)
		if res.get('status', 'error') == 'error' and self.exception_on_error:
			raise Connection.ErrorFromDB("PhaazeDB returned: "+str(res))

		return res

	def select(self, of=None, where="", fields=[]):
		if of == None: raise AttributeError("'of' can't be None")
		if type(fields) is not list: AttributeError("'fields' must be list")

		call = dict(
			action="select",
			token=self.token,
			of=of,
			where=where,
			fields=fields)

		try: r = self.session.post(self.adress+self.port, json=call)
		except: raise ConnectionError("Failed to connect")

		res = json.loads(r.text)
		if res.get('status', 'error') == 'error' and self.exception_on_error:
			raise Connection.ErrorFromDB("PhaazeDB returned: "+str(res))

		return res

	class ErrorFromDB(Exception):
		pass
