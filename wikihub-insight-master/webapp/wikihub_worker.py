#!/usr/bin/env python
from app import app
from flask.ext.script import Manager, Shell

manager = Manager(app)

def make_shell_context():
    return dict(app=app)

manager.add_command('shell', Shell(make_context=make_shell_context))

if __name__ == '__main__':
    manager.run()

# if __name__ == '__main__':
#     app.run(host='0.0.0.0', debug = True)
