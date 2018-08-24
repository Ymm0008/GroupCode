# -*- coding: utf-8 -*-

from optparse import OptionParser

from apt import create_app

optparser = OptionParser()
optparser.add_option('-p', '--port', dest='port', help='Server Http Port Number', default=9002, type='int')
(options, args) = optparser.parse_args()

app = create_app()

if __name__ == '__main__':
    app.run(host='219.224.134.220', port=options.port, debug=True)
