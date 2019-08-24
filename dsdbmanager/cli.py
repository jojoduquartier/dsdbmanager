import click


@click.group()
def main():
    """

    :return:
    """

    return "data science database connection manager"


@main.command()
def add_database():
    from dsdbmanager.configuring import ConfigFilesManager
    manager = ConfigFilesManager()
    manager.add_new_database_info()


@main.command()
def remove_database():
    from dsdbmanager.configuring import ConfigFilesManager
    manager = ConfigFilesManager()
    manager.remove_database()


@main.command()
def reset_credentials():
    from dsdbmanager.configuring import ConfigFilesManager
    manager = ConfigFilesManager()
    manager.reset_credentials()
