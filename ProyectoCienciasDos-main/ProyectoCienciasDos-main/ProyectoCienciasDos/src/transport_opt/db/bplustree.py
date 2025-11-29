
from typing import Any, List, Optional, Tuple

class BPlusTreeNode:
    def __init__(self, leaf: bool = False):
        self.leaf = leaf
        self.keys: List[str] = []
        self.children: List[Any] = []  # values or child nodes
        self.next: Optional['BPlusTreeNode'] = None

class BPlusTree:
    """B+ Tree simplificado. API: insert(key, value), search(key), traverse_leaves()."""

    def __init__(self, order: int = 4):
        if order < 3:
            raise ValueError("order must be >= 3")
        self.root = BPlusTreeNode(leaf=True)
        self.order = order

    def _find_leaf(self, node: BPlusTreeNode, key: str) -> BPlusTreeNode:
        if node.leaf:
            return node
        for i, k in enumerate(node.keys):
            if key < k:
                return self._find_leaf(node.children[i], key)
        return self._find_leaf(node.children[-1], key)

    def search(self, key: str) -> Optional[Any]:
        leaf = self._find_leaf(self.root, key)
        for i, k in enumerate(leaf.keys):
            if k == key:
                return leaf.children[i]
        return None

    def insert(self, key: str, value: Any) -> None:
        leaf = self._find_leaf(self.root, key)
        idx = 0
        while idx < len(leaf.keys) and leaf.keys[idx] < key:
            idx += 1
        if idx < len(leaf.keys) and leaf.keys[idx] == key:
            leaf.children[idx] = value
            return
        leaf.keys.insert(idx, key)
        leaf.children.insert(idx, value)
        if len(leaf.keys) >= self.order:
            self._split_leaf(leaf)

    def _split_leaf(self, leaf: BPlusTreeNode) -> None:
        new_leaf = BPlusTreeNode(leaf=True)
        mid = len(leaf.keys) // 2
        new_leaf.keys = leaf.keys[mid:]
        new_leaf.children = leaf.children[mid:]
        leaf.keys = leaf.keys[:mid]
        leaf.children = leaf.children[:mid]
        new_leaf.next = leaf.next
        leaf.next = new_leaf

        if leaf is self.root:
            new_root = BPlusTreeNode(leaf=False)
            new_root.keys = [new_leaf.keys[0]]
            new_root.children = [leaf, new_leaf]
            self.root = new_root
        else:
            self._insert_in_parent(self.root, leaf, new_leaf.keys[0], new_leaf)

    def _insert_in_parent(self, node: BPlusTreeNode, left_child: BPlusTreeNode, key: str, right_child: BPlusTreeNode) -> None:
        if node.leaf:
            return
        for i, c in enumerate(node.children):
            if c is left_child:
                insert_pos = i + 1
                break
        else:
            for c in node.children:
                if not c.leaf:
                    self._insert_in_parent(c, left_child, key, right_child)
            return

        node.keys.insert(insert_pos - 1, key)
        node.children.insert(insert_pos, right_child)
        if len(node.children) > self.order:
            self._split_internal(node)

    def _split_internal(self, node: BPlusTreeNode) -> None:
        mid = len(node.keys) // 2
        new_internal = BPlusTreeNode(leaf=False)
        promote_key = node.keys[mid]
        new_internal.keys = node.keys[mid+1:]
        new_internal.children = node.children[mid+1:]
        node.keys = node.keys[:mid]
        node.children = node.children[:mid+1]

        if node is self.root:
            new_root = BPlusTreeNode(leaf=False)
            new_root.keys = [promote_key]
            new_root.children = [node, new_internal]
            self.root = new_root
        else:
            self._insert_in_parent(self.root, node, promote_key, new_internal)

    def traverse_leaves(self) -> List[Tuple[str, Any]]:
        node = self.root
        while not node.leaf:
            node = node.children[0]
        res = []
        while node:
            for k, v in zip(node.keys, node.children):
                res.append((k, v))
            node = node.next
        return res