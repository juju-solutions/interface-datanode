# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json

from charms.reactive import RelationBase
from charms.reactive import hook
from charms.reactive import scopes
from charms.reactive.bus import get_states

from charmhelpers.core import hookenv


class DataNodeProvides(RelationBase):
    scope = scopes.GLOBAL
    auto_accessors = ['host', 'port', 'webhdfs-port', 'ssh-key']

    def set_spec(self, spec):
        """
        Set the local spec.

        Should be called after ``{relation_name}.related``.
        """
        conv = self.conversation()
        conv.set_local('spec', json.dumps(spec))

    def local_hostname(self):
        return hookenv.local_unit().replace('/', '-')

    def spec(self):
        return json.loads(self.get_remote('spec', '{}'))

    def hosts_map(self):
        return json.loads(self.get_remote('hosts-map', '{}'))

    @hook('{provides:datanode}-relation-joined')
    def joined(self):
        self.set_state('{relation_name}.related')

    @hook('{provides:datanode}-relation-changed')
    def changed(self):
        hookenv.log('Data: {}'.format({
            'spec': self.spec(),
            'host': self.host(),
            'port': self.port(),
            'webhdfs_port': self.webhdfs_port(),
            'hosts_map': self.hosts_map(),
            'local_hostname': self.local_hostname(),
        }))
        available = all([self.spec(), self.host(), self.port(), self.webhdfs_port(), self.ssh_key()])
        spec_matches = self._spec_match()
        registered = self.local_hostname() in self.hosts_map().values()

        self.toggle_state('{relation_name}.spec.mismatch', available and not spec_matches)
        self.toggle_state('{relation_name}.ready', available and spec_matches and registered)

        hookenv.log('States: {}'.format(get_states().keys()))

    def register(self):
        self.set_remote('registered', 'true')

    @hook('{provides:datanode}-relation-{departed,broken}')
    def departed(self):
        self.remove_state('{relation_name}.related')
        self.remove_state('{relation_name}.spec.mismatch')
        self.remove_state('{relation_name}.ready')

    def _spec_match(self):
        conv = self.conversation()
        local_spec = json.loads(conv.get_local('spec', '{}'))
        remote_spec = json.loads(conv.get_remote('spec', '{}'))
        for key, value in local_spec.items():
            if value != remote_spec.get(key):
                return False
