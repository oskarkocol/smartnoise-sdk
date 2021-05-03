import itertools
from typing import Generic, Union, List, Any, Optional, Dict, Iterator, Tuple


class Token(str):
    def __init__(self, text: str):
        self.text = text

    def __str__(self) -> str:
        return self.text

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, str):
            return self.text == other
        return type(self) == type(other) and self.text == other.text

    def children(
        self,
        ) -> List[Optional[Union['Token', 'Op', 'Identifier', 'FuncName', 'Sql']]]:
        return [None]

    def __hash__(self) -> int:
        return hash(self.text)


class Op(str):
    def __init__(self, text: str):
        self.text = text

    def __str__(self) -> str:
        return self.text

    def __eq__(self, other):
        if isinstance(other, str):
            return self.text == other
        return type(self) == type(other) and self.text == other.text

    def children(
        self,
        ) -> List[Optional[Union['Token', 'Op', 'Identifier', 'FuncName', 'Sql']]]:
        return [None]

    def __hash__(self) -> int:
        return hash(self.text)


class Identifier(str):
    def __init__(self, text: str):
        self.text = text

    def __str__(self) -> str:
        return self.text

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, str):
            return self.text == other
        return type(self) == type(other) and self.text == other.text

    def children(
        self,
        ) -> List[Optional[Union['Token', 'Op', 'Identifier', 'FuncName', 'Sql']]]:
        return [None]

    def __hash__(self) -> int:
        return hash(self.text)


class FuncName(str):
    def __init__(self, text: str):
        self.text = text

    def __str__(self) -> str:
        return self.text

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, str):
            return self.text == other
        return type(self) == type(other) and self.text == other.text

    def children(
        self,
        ) -> List[Optional[Union['Token', 'Op', 'Identifier', 'FuncName', 'Sql']]]:
        return [None]

    def __hash__(self) -> int:
        return hash(self.text)


class Sql:
    """
        base type for all Sql AST nodes
    """

    def __str__(self) -> str:
        return " ".join([str(c) for c in self.children() if c is not None])

    def __eq__(self, other) -> bool:
        if other is None:
            return False
        else:
            return all([s == o for s, o in zip(self.children(), other.children())])

    def symbol_name(self) -> str:
        return str(hex(hash(self) % (2 ** 16)))

    def __hash__(self) -> int:
        return hash(tuple(self.children()))

    def children(
        self,
        ) -> List[Optional[Union['Token', 'Op', 'Identifier', 'FuncName', 'Sql']]]:
        return []

    def find_node(self, type_name: Any) -> Union[None, Token, Op, Identifier, FuncName, 'Sql']:
        """
            Walks the tree and returns the first node
            that is an instance of the specified type.
        """
        candidates = [c for c in self.children() if c is not None]
        for c in candidates:
            if isinstance(c, type_name):
                return c
            if isinstance(c, Sql):
                n = c.find_node(type_name)
                if n is not None:
                    return n
        return None

    def find_nodes(self, type_name: Any, not_child_of: Any=None) -> List[Union[None, Token, Op, Identifier, FuncName, 'Sql']]:
        """
            Walks the tree and returns all nodes
            that are an instance of the specified type.
        """
        candidates = [c for c in self.children() if c is not None]
        nodes = [c for c in candidates if isinstance(c, type_name)]
        sqlnodes = [c for c in candidates if isinstance(c, Sql)]
        if not_child_of is not None:
            sqlnodes = [c for c in sqlnodes if not isinstance(c, not_child_of)]
        childnodes = [c.find_nodes(type_name, not_child_of) for c in sqlnodes]
        return nodes + flatten(childnodes)

    def visualize(self, color_types: Dict[Any, 'str']={}, n_trunc: Optional[int]=None) -> 'Digraph': # type: ignore
        """
        Construct the AST graph of the object

        Args:
            color_type (optional): A dictionnary which contains the type and
                the color of the nodes.
                If no color is provided, the displayed node will be black.
                Example: {str: 'red', Query: 'green'}
            n_trunc: for visibility, truncate the expressions.
                By default, there is no truncation.

        Returns:
            graphviz Digraph

        """
        def _label_node(expr: Any, n_trunc: Optional[int]) -> str:
            str_expr = str(expr)
            if n_trunc and len(str_expr) > n_trunc:
                str_expr = str_expr[:n_trunc] + '...'
            return f"{type(expr).__name__}: {str_expr}"

        def _color_node(expr: Any, color_types: Dict[Any, 'str']) -> str:
            expr_type = type(expr)
            if expr_type in color_types.keys():
                return color_types[expr_type]
            return 'black'

        def _visit_nodes(node: Any, path_to_node: str) -> None:
            if not isinstance(node, Sql):
                return
            for child_node in node.children():
                if child_node is not None:
                    path_to_child_node = f"{path_to_node}.{str(child_node)}_{str(id(child_node))}"
                    graph.node( # type: ignore
                        path_to_child_node,
                        _label_node(child_node, n_trunc),
                        color=_color_node(child_node, color_types),
                    )
                    graph.edge(path_to_node, path_to_child_node) # type: ignore
                    _visit_nodes(child_node, path_to_child_node)

        from graphviz import Digraph # type: ignore
        graph = Digraph()
        graph.node(
            str(self),
            _label_node(self, n_trunc),
            color=_color_node(self, color_types),
        )
        _visit_nodes(self, str(self))
        return graph


class Seq(Sql):
    def __init__(self, seq: List[Sql]):
        self.seq = seq

    def __str__(self) -> str:
        return ", ".join([str(c) for c in self.seq if c is not None])

    def __eq__(self, other: Any) -> bool:
        return all([s == o for s, o in zip(self.seq, other.seq)])

    def __len__(self) -> int:
        return len(self.seq)

    def __getitem__(self, key: int) -> Sql:
        return self.seq[key]

    def __setitem__(self, key: int, value: Sql):
        self.seq[key] = value

    def __iter__(self) -> Iterator[Sql]:
        return iter(self.seq)

    def children(
        self,
        ) -> List[Optional[Union['Token', 'Op', 'Identifier', 'FuncName', 'Sql']]]:
        return self.seq

    def symbol(self, relations): # type: ignore
        return Seq([i.symbol(relations) for i in self.seq])


class SqlRel(Sql):
    """
        base type for all SQL relations
    """

    def __init__(self):
        self.m_symbols = None #: Optional[List[Tuple[Identifier, 'SqlExpr']]]
        self.m_sym_dict = None #: Optional[Dict[Identifier, 'SqlExpr']]

    def has_symbols(self) -> bool:
        if hasattr(self, "m_symbols"):
            return self.m_symbols is not None
        return any([r.has_symbols() for r in self.relations()])

    def alias_match(self, name: str) -> bool:
        orig_name = name
        alias, name = self.split_alias(name)
        if alias.strip() == "":
            return True
        if hasattr(self, "alias"):
            if self.alias is None: # type: ignore
                return False
            else:
                return self.alias.lower() == alias.lower()# type: ignore
        else:
            return any([r.alias_match(orig_name) for r in self.relations()])

    def split_alias(self, name: str) -> Tuple[str, str]:
        parts = name.split(".")
        if len(parts) > 2:
            raise ValueError("Identifiers can have only two parts: " + name)
        elif len(parts) == 2:
            return (parts[0], parts[1])
        else:
            return ("", parts[0])

    def __contains__(self, key: Identifier) -> bool:
        if not self.has_symbols():
            return False
        if self.m_sym_dict is None:
            self.m_sym_dict = dict(self.m_symbols)
        return key in self.m_sym_dict

    def __getitem__(self, key: Identifier) -> 'SqlExpr':
        if not self.has_symbols():
            raise ValueError("No symbols loaded")
        if self.m_sym_dict is None:
            self.m_sym_dict = dict(self.m_symbols)
        return self.m_sym_dict[key]

    def load_symbols(self, metadata): # type: ignore
        for r in self.relations():
            r.load_symbols(metadata)

    def all_symbols(self, expression=None):
        if not self.has_symbols():
            raise ValueError(
                "Cannot load symbols from a table with no metadata.  Check has_symbols, or use load_symbols with metadata first. "
                + str(self)
            )
        else:
            return self.m_symbols

    def relations(self) -> List['SqlRel']:
        return [r for r in self.children() if isinstance(r, SqlRel)]


class SqlExpr(Sql):
    """
        Base type for all SQL expressions
    """

    def type(self) -> str:
        return "unknown"

    def sensitivity(self) -> Optional[Union[float, int]]:
        return None

    def evaluate(self, bindings: Dict[str, Union[int, float, bool, str]]) -> Optional[Union[int, float, bool, str]]:
        raise ValueError("We don't know how to evaluate " + str(self))

    def children(
        self,
        ) -> List[Optional[Union['Token', 'Op', 'Identifier', 'FuncName', 'Sql']]]:
        return [None]

    def symbol(self, relations):
        # force inherited class to override if Column children exist
        child_col = self.find_node(Column)
        if child_col is not None:
            raise ValueError(
                "Symbol not implemented on: "
                + str(self)
                + " even though has Sql Column child "
                + str(child_col)
            )
        return self

    @property
    def is_key_count(self) -> bool:
        return False

    @property
    def is_count(self) -> bool:
        return False


class Literal(SqlExpr):
    """A literal used in an expression"""

    def __init__(self, value: Union[int, float, str, None], text: Optional[str]=None):
        if text is None:
            self.value = value
            if value is None:
                self.text = "NULL"
            else:
                self.text = str(value)
        else:
            if not isinstance(text, str):
                self.value = text
                self.text = str(value)
            else:
                self.text = text
                self.value = value

    def __str__(self) -> str:
        return self.text

    def __eq__(self, other: Any) -> bool:
        return type(self) == type(other) and self.text == other.text

    def __hash__(self) -> int:
        return hash(self.text)

    def symbol(self, relations):
        return self

    def type(self) -> str:
        if isinstance(self.value, str):
            return "string"
        elif type(self.value) is float:
            return "float"
        elif type(self.value) is int:
            return "int"
        elif type(self.value) is bool:
            return "boolean"
        else:
            raise ValueError("Unknown literal type: " + str(type(self.value)))

    def evaluate(self, bindings: Dict[str, Union[int, float, bool, str]]) -> Optional[Union[int, float, bool, str]]:
        return self.value


class Column(SqlExpr):
    """A fully qualified column name"""

    def __init__(self, name: str):
        self.name = name

    def __str__(self) -> str:
        return str(self.name)

    def __eq__(self, other: Any) -> bool:
        return type(self) == type(other) and self.name == other.name

    def __hash__(self) -> int:
        return hash(self.name)

    def escaped(self) -> bool:
        # is any part of this identifier escaped?
        parts = self.name.split(".")
        return any([p.startswith('"') or p.startswith("[") for p in parts])

    def symbol_name(self) -> str:
        return self.name

    def symbol(self, relations):
        sym = [r.symbol(self) for r in relations if r.alias_match(self.name)]
        if len(sym) == 0:
            raise ValueError("Column cannot be found " + str(self))
        elif len(sym) > 1:
            raise ValueError("Column matches more than one relation, ambiguous " + str(self))
        else:
            return sym[0]

    def evaluate(self, bindings: Dict[str, Union[int, float, bool, str]]) -> Optional[Union[int, float, bool, str]]:
        # may need to handle escaping here
        if str(self).lower().replace('"', "") in bindings:
            return bindings[str(self).lower().replace('"', "")]
        else:
            return None


def flatten(iter: Any) -> List[Any]:
    return list(itertools.chain.from_iterable(iter))


def unique(iter: Any) -> List[Any]:
    return list(set(iter))
