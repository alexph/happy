import optparse

import os
import subprocess


class CommandRunner(object):
    whitelist = ['start']

    def main(self):
        usage = 'usage: %prog [options] command arg1 arg2'
        parser = optparse.OptionParser(usage)

        (options, args) = parser.parse_args()

        if len(args) < 1:
            parser.error('Command missing - specify which command you wish to run %s'
                         % self.display_commands())

        command = args[0]

        if not command in self.whitelist:
            parser.error('Command does not exist %s' % self.display_commands())
        else:
            if command == 'start':
                self.start_file(parser, args)

    def display_commands(self):
        return '\n\nAvailable commands: \n%s' % '\n'.join(
            ['* %s' % x for x in self.whitelist])

    def start_file(self, parser, args):
        if len(args) < 2:
            parser.error('Argument one should be the python topology file to submit')

        loc = os.path.realpath(args[1])

        if not os.path.exists(loc):
            parser.error('File does not exist')

        subprocess.check_call(['python', loc])
