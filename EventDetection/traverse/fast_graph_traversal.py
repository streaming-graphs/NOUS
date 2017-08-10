import platform


if platform.system() == 'Darwin':
    from mac_graph_traversal import *
else:
    from graph_traversal import *
