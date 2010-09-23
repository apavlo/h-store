class XMLException(Exception):
    """General XML Exception.
    """

    pass

class InvalidXML(XMLException):
    """Invalid XML Exception.

    This is raised when the XML is mal-formed.
    """

    pass

class InvalidObject(XMLException):
    """Invalid Python object exception.

    This is raised when the Python object passed to the XML generator is invalid.
    """

    pass
