__author__ = 'johann'

import fbconsole

from conf.confreader import ConfReader

class FacebookAuthenticator:

    def __init__(self):
        self.authentication_data_path = ConfReader.AUTHENTICATION_DATA_PATH
        self.saved_access_token_path = ConfReader.SAVED_ACCESS_TOKEN_PATH

    def get_user_credentials(self, path):

        file = open(path, "r+")
        file_content = file.readlines()

        for line in file_content:
            if "login" in line:
                login = line.rsplit("login")[1].replace(":", "").replace(" ", "").rstrip()
            if "password" in line:
                password = line.rsplit("password")[1].replace(":", "").replace(" ", "").rstrip()

        if "login" in locals() and "password" in locals():
            return {"login" : login, "password" : password}
        else:
            raise NameError("Login or password not found inside %(path)s." % {"path" : path})

    def get_saved_access_token(self):
        file = open(self.saved_access_token_path, "r+")
        access_token = file.readlines()[0].strip()
        file.close()

        return access_token

    def get_new_access_token(self):
        return self.request_app_access_token(self.get_authentication_data(self.authentication_data_path))

    def get_authentication_data(self, path):
        file = open(path, "r+")

        file_content = file.readlines()

        for line in file_content:
            if "app_id" in line:
                app_id = line.rsplit("app_id")[1].replace("-", "").replace(" ", "").rstrip()
            if "username" in line:
                username = line.rsplit("username")[1].replace("-", "").replace(" ", "").rstrip()
            if "password" in line:
                password = line.rsplit("password")[1].replace("-", "").replace(" ", "").rstrip()
            if "app_secret" in line:
                app_secret = line.rsplit("app_secret")[1].replace("-", "").replace(" ", "").rstrip()
            if "redirect_url" in line:
                redirect_url = line.rsplit("redirect_url")[1].replace("-", "").replace(" ", "").rstrip()

        if "app_id" in locals() and "username" in locals() and "password" in locals() and "app_secret" in locals()\
            and "redirect_url" in locals():
            return {"app_id" : app_id, "username" : username, "password" : password, "app_secret" : app_secret,
                    "redirect_url" : redirect_url}
        else:
            raise NameError("There is missing data inside %(path)s. Check if there is: app_id, username, password, "
                            "app_secret and redirect_url inside the file." % {"path" : path})

    def request_app_access_token(self, authentication_data):
        fbconsole.AUTH_SCOPE = ['read_stream']

        app_id = authentication_data["app_id"]
        username = authentication_data["username"]
        password = authentication_data["password"]
        app_secret = authentication_data["app_secret"]
        redirect_uri = authentication_data["redirect_url"]

        fbconsole.APP_ID = app_id
        fbconsole.automatically_authenticate(
            username = username,     # facebook username for authentication
            password = password,     # facebook password for authentication
            app_secret = app_secret,   # "app secret" from facebook app settings
            redirect_uri = redirect_uri # redirect uri specified in facebook app settings
        )

        self.save_access_token(fbconsole.ACCESS_TOKEN)

        return fbconsole.ACCESS_TOKEN

    def save_access_token(self, new_access_token):
        file = open(self.saved_access_token_path, "w+")
        file.writelines(new_access_token)
        file.close()

if __name__ == '__main__':
    pass
