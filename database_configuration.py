
class DatabaseConfiguration:
    """
    A database configuration.
    Parameters
    ----------
    host : str
        Domain of the host server (such as 127.0.0.1 or db.your.host)
    username : str
        Username of the database user.
    password : str
        Password of the database user.
    database : str
        Name of the database.
    port : INT
        Port number.
    """

    def __init__(
            self, host: str, username: str, password: str, database: str, port: int
    ) -> None:

        self._host = host
        self._username = username
        self._password = password
        self._database = database
        self._port = port

    def host(self) -> str:
        """
        The domain of the host server.
        Returns
        -------
        str
            The host server.
        """

        return self._host

    def username(self) -> str:
        """
        The username of the database user.
        Returns
        -------
        str
            The database user.
        """

        return self._username

    def password(self) -> str:
        """
        The password of the database user.
        Returns
        -------
        str
            The password.
        """

        return self._password

    def database(self) -> str:
        """
        The name of the database.
        Returns
        -------
        str
            The name of the database.
        """

        return self._database

    def port(self) -> int:
        """
        The port number.
        Returns
        -------
        INT
            The port number.
        """

        return self._port
