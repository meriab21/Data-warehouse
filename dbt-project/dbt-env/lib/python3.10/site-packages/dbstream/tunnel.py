import os
from sshtunnel import SSHTunnelForwarder, HandlerSSHTunnelForwarderError, create_logger
from dbstream.tools.print_colors import C


def create_ssh_tunnel(instance, port, remote_host, remote_port):
    # Create an SSH tunnel
    ssh_host = os.environ["SSH_%s_HOST" % instance]
    ssh_user = os.environ["SSH_%s_USER" % instance]
    try:
        ssh_private_key = os.environ["SSH_%s_PRIVATE_KEY" % instance]
        ssh_private_key = ssh_private_key.replace('\\n', '\n')
        ssh_path_private_key = 'ssh_path_private_key'
        with open(ssh_path_private_key, 'w') as w:
            w.write(ssh_private_key)
            w.close()
    except KeyError:
        ssh_path_private_key = os.environ["SSH_%s_PATH_PRIVATE_KEY" % instance]
        print(C.OKBLUE + 'Private Key used' + C.ENDC)

    tunnel = SSHTunnelForwarder(
        (ssh_host, 22),
        ssh_username=ssh_user,
        ssh_pkey=ssh_path_private_key,
        remote_bind_address=(remote_host, int(remote_port)),
        local_bind_address=('localhost', port),  # could be any available port
    )
    try:
        tunnel.start()
        print(C.OKBLUE + 'Tunnel opened and started [<<<<<]' + C.ENDC)
        return tunnel
    except HandlerSSHTunnelForwarderError:
        print(C.OKBLUE + 'HandlerSSHTunnelForwarderError' + ' [<<<<<]' + C.ENDC)
        port = port + 1
        return create_ssh_tunnel(instance=instance, port=port, remote_port=remote_port, remote_host=remote_host)

