import base64
import keyring

user_ns_id = 'p2p'
user_key = 'p2p_user'
pw_key = 'p2p_password'
id_key = 'p2p_id'
secret_key = 'p2p_secret'

class ProsperClient():
	username = base64.b64decode(keyring.get_password(user_ns_id, user_key)).decode('utf-8')
	password = base64.b64decode(keyring.get_password(user_ns_id, pw_key)).decode('utf-8')
	id = base64.b64decode(keyring.get_password(user_ns_id, id_key)).decode('utf-8')
	secret = base64.b64decode(keyring.get_password(user_ns_id, secret_key)).decode('utf-8')