from xml.dom.minidom import Document

from exceptions import *

class XMLGenerator:
    """The XML generator class.

    This class generates the XML which complies with our schema.
    """

    __root_name = "SQLGenerator"
    __attributes = ("id",)
    __parallels = {"Statements": "Statement"}

    def __init__(self, obj):
        """Constructor.

        'obj': The Python object to be transformed into XML.
        """

        self.obj = obj
        self.tree = None

    def __buildinner__(self, current, pos):
        """Builds the inner branches of the tree.

        'current': The current node.
        'pos': The current position in the Python object.
        """

        if type(pos) is dict:
            for k, v in pos.iteritems():
                if k in self.__attributes:
                    if type(v) is not unicode:
                        current.setAttribute(k, str(v))
                    else:
                        current.setAttribute(k, v)
                else:
                    node = self.tree.createElement(k)
                    current.appendChild(node)
                    self.__buildinner__(node, v)
        elif type(pos) is list:
            for v in pos:
                node = self.tree.createElement(self.__parallels[current.tagName])
                current.appendChild(node)
                self.__buildinner__(node, v)
        else:
            if not isinstance(pos, basestring):
                current.appendChild(self.tree.createTextNode(str(pos)))
            else:
                current.appendChild(self.tree.createTextNode(pos))

    def __buildtree__(self):
        """Builds the DOM tree.
        """

        root = self.tree.createElement(self.__root_name)
        self.tree.appendChild(root)

        self.__buildinner__(root, self.obj)

    def toXML(self):
        """Outputs the XML representation of the Python object.

        Return: The XML data.
        """

        if type(self.obj) is not dict:
            raise InvalidObject()

        if not self.tree:
            self.tree = Document()
            self.__buildtree__()

        return self.tree.toprettyxml("  ", encoding = "utf-8")
