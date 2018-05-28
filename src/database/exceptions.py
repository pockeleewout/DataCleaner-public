

class DataError(Exception):
    """Base error used by the Data class"""
    pass


class VersionError(DataError):
    """Base error used by the DataVersion class"""
    pass


class TableError(DataError):
    """Base error used by the DataTable class"""
    pass


class JoinError(TableError):
    """Error returned when joining of tables fails"""
    pass
