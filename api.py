import json

from webob import Response
from ryu.app.wsgi import ControllerBase, route
from ryu.lib import dpid as dpid_lib

class RestAPI(ControllerBase):
    def __init__(self, req, link, data, **config):
        super(RestAPI, self).__init__(req, link, data, **config)
        self.valve = data['valve']

    @route('valve', '/datapaths',
            methods=['GET'])
    def list_datapaths(self, req, **kwargs):
        dps = {}

        for dpid, dp in self.valve.dps.items():
            dps[dpid] = {'is_connected': False}

        for dp in self.valve.dpset.get_all():
            dpid = dp[0]
            dps.setdefault(dpid, {})
            dps[dpid]['is_connected'] = dp[1].is_active
            dps[dpid]['address'] = dp[1].address

        body = json.dumps(dps)
        return Response(content_type='application/json', body=body)

    @route('valve', '/datapath/{dpid}/mactable',
            methods=['GET'], requirements={'dpid': dpid_lib.DPID_PATTERN})
    def list_mac_table(self, req, **kwargs):
        dpid = dpid_lib.str_to_dpid(kwargs['dpid'])

        if dpid not in self.valve.dps:
            print "valid dps: "
            print self.valve.dps
            return Response(status=404)

        mac_table = self.valve.mac_to_port.get(dpid, {})
        body = json.dumps(mac_table)
        return Response(content_type='application/json', body=body)

    @route('valve', '/datapath/{dpid}/vlan/{vlan}/mactable',
            methods=['GET'], requirements={'dpid': dpid_lib.DPID_PATTERN})
    def list_vlan_mac_table(self, req, **kwargs):
        dpid = dpid_lib.str_to_dpid(kwargs['dpid'])
        vlan = int(kwargs['vlan'])

        if dpid not in self.valve.dps:
            return Response(status=404)

        mac_table = self.valve.mac_to_port.get(dpid, {}).get(vlan, {})
        print mac_table
        body = json.dumps(mac_table)
        return Response(content_type='application/json', body=body)
