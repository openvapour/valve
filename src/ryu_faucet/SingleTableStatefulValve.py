import logging
from ryu.ofproto import ether
from ryu.ofproto import ofproto_v1_3 as ofp
from ryu.ofproto import ofproto_v1_3_parser as parser
from ryu_faucet.valve import Valve
from ryu_faucet.util import mac_addr_is_unicast

class SingleTableStatefulValve(Valve):
    """Single Table implementation of Valve.

    Stateful as the controller must keep state in order to provision flows
    correctly.
    """
    def __init__(self, dp, logname='faucet', *args, **kwargs):
        self.dp = dp
        self.logger = logging.getLogger(logname)

        # Initialise datastructures to store state
        self.mac_to_port = {}

    def tagged_output_action(self, parser, tagged_ports):
        act = []
        for port in tagged_ports:
            if port.running():
                act.append(parser.OFPActionOutput(port.number))
        return act

    def untagged_output_action(self, parser, untagged_ports):
        act = []
        for port in untagged_ports:
            if port.running():
                act.append(parser.OFPActionOutput(port.number))
        return act

    def send_config(self):
        self.logger.info("Configuring datapath")

        # of messgaes to send to switch
        ofmsgs = []

        # default values for flowmods
        datapath = self.dp
        cookie = datapath.cookie
        match_all = parser.OFPMatch()

        # clear flow table on datapath
        ofmsgs.append(
            parser.OFPFlowMod(
                datapath=None,
                cookie=cookie,
                command=ofp.OFPFC_DELETE,
                out_port=ofp.OFPP_ANY,
                out_group=ofp.OFPG_ANY,
                match=match_all
                )
            )

        # add catchall drop rule to datapath
        ofmsgs.append(
            parser.OFPFlowMod(
                datapath=None,
                cookie=cookie,
                priority=datapath.lowest_priority,
                match=match_all,
                instructions=[]
                )
            )


        for vid, v in datapath.vlans.items():
            self.logger.info("Configuring %s", v)

            controller_act = parser.OFPActionOutput(ofp.OFPP_CONTROLLER)

            # generate the output actions for each port
            tagged_act = self.tagged_output_action(parser, v.tagged)
            untagged_act = self.untagged_output_action(parser, v.untagged)

            # send rule for matching packets arriving on tagged ports
            for port in v.tagged:
                strip_act = [parser.OFPActionPopVlan()]
                action = [controller_act]
                if tagged_act:
                    action += tagged_act
                if untagged_act:
                    action += strip_act + untagged_act
                match = parser.OFPMatch(in_port=port.number, vlan_vid=v.vid|ofp.OFPVID_PRESENT)
                instruction = parser.OFPInstructionActions(ofp.OFPIT_APPLY_ACTIONS, action)
                ofmsgs.append(
                    parser.OFPFlowMod(
                        datapath=None,
                        cookie=cookie,
                        priority=datapath.low_priority,
                        match=match,
                        instructions=[instruction]
                        )
                    )

            # send rule for each untagged port
            push_act = [
                parser.OFPActionPushVlan(ether.ETH_TYPE_8021Q),
                parser.OFPActionSetField(vlan_vid=v.vid|ofp.OFPVID_PRESENT)
                ]

            for port in v.untagged:
                match = parser.OFPMatch(in_port=port.number)
                action = []
                if untagged_act:
                    action += untagged_act
                if tagged_act:
                    action += push_act + tagged_act
                action.append(controller_act)
                instruction = parser.OFPInstructionActions(
                    ofp.OFPIT_APPLY_ACTIONS,
                    action
                    )
                ofmsgs.append(
                    parser.OFPFlowMod(
                        datapath=None,
                        cookie=cookie,
                        priority=datapath.low_priority,
                        match=match,
                        instructions=[instruction]
                        )
                    )

        # Mark datapath as fully configured
        datapath.running = True

        self.logger.info("Datapath configured")

        return ofmsgs

    def reload_config(self, new_dp):
        old_dp = self.dp

        new_dp.running = old_dp.running

        self.dp = new_dp

        if not self.dp.running:
            return []

        for portnum, port in old_dp.ports.iteritems():
            if portnum in new_dp.ports:
                new_dp.ports[portnum].phys_up = port.phys_up
            else:
                port.enabled = False
                new_dp.ports[portnum] = port

        return self.send_config()

    def rcv_packet(self, dp_id, in_port, vlan_vid, eth_src, eth_dst):
        # TODO: brad will work out how to deal with timeouts. Thanks Brad!
        self.mac_to_port.setdefault(dp_id, {})

        if dp_id != self.dp.dp_id:
            self.logger.error("Packet_in on unknown datapath")
            return []
        else:
            datapath = self.dp

        if not datapath.running:
            self.logger.error("Packet_in on unconfigured datapath")

        if in_port not in datapath.ports:
            self.set_default_log_formatter()
            return []

        if not mac_addr_is_unicast(eth_src):
            self.logger.info("Packet_in with multicast ethernet source address")
            return []

        self.mac_to_port[dp_id].setdefault(vlan_vid, {})

        self.logger.info("Packet_in dp_id:%x src:%s dst:%s in_port:%d vid:%s",
                         dp_id, eth_src, eth_dst, in_port, vlan_vid)

        # flow_mods to be installed
        ofmsgs = []

        # learn a mac address to avoid FLOOD next time.
        self.mac_to_port[dp_id][vlan_vid][eth_src] = in_port

        # generate the output actions for broadcast traffic from this host
        tagged_act = self.tagged_output_action(parser, datapath.vlans[vlan_vid].tagged)
        untagged_act = self.untagged_output_action(parser, datapath.vlans[vlan_vid].untagged)

        match = None
        action = []
        if datapath.vlans[vlan_vid].port_is_tagged(in_port):
            # send rule for mathcing packets arriving on tagged ports
            strip_act = [parser.OFPActionPopVlan()]
            if tagged_act:
                action += tagged_act
            if untagged_act:
                action += strip_act + untagged_act

            match = parser.OFPMatch(
                vlan_vid=vlan_vid|ofp.OFPVID_PRESENT,
                in_port=in_port,
                eth_src=eth_src,
                eth_dst=('01:00:00:00:00:00','01:00:00:00:00:00')
                )
        elif datapath.vlans[vlan_vid].port_is_untagged(in_port):
            # send rule for each untagged port
            push_act = [
              parser.OFPActionPushVlan(ether.ETH_TYPE_8021Q),
              parser.OFPActionSetField(vlan_vid=vlan_vid|ofp.OFPVID_PRESENT)
              ]
            if untagged_act:
                action += untagged_act
            if tagged_act:
                action += push_act + tagged_act

            match = parser.OFPMatch(in_port=in_port,
                    eth_src=eth_src,
                    eth_dst=('01:00:00:00:00:00',
                             '01:00:00:00:00:00'))

        # install broadcast/multicast rules onto datapath
        flood_instruction = parser.OFPInstructionActions(ofp.OFPIT_APPLY_ACTIONS, action)
        priority = datapath.low_priority
        cookie = datapath.cookie
        flowmod = parser.OFPFlowMod(
            datapath=None,
            cookie=cookie,
            priority=priority,
            match=match,
            instructions=[flood_instruction]
            )
        ofmsgs.append(flowmod)

        # install unicast flows onto datapath
        if eth_dst in self.mac_to_port[dp_id][vlan_vid]:
            # install a flow to avoid packet_in next time
            out_port = self.mac_to_port[dp_id][vlan_vid][eth_dst]
            if out_port == in_port:
                self.logger.info(
                    "in_port is the same as out_port, skipping unicast flow " \
                    "dp_id: %x dl_dst:%s dl_src:%s vid:%d",
                    dp_id,
                    eth_dst,
                    eth_src,
                    vlan_vid)
                return

            self.logger.info("Adding unicast flow dl_dst:%s vid:%d", eth_dst, vlan_vid)

            actions = []

            if datapath.ports[in_port].is_tagged():
                match = parser.OFPMatch(
                    in_port=in_port,
                    eth_src=src,
                    eth_dst=dst,
                    vlan_vid=vid|ofp.OFPVID_PRESENT)
                if datapath.ports[out_port].is_untagged():
                    actions.append(parser.OFPActionPopVlan())
            if datapath.ports[in_port].is_untagged():
                match = parser.OFPMatch(
                    in_port=in_port,
                    eth_src=src,
                    eth_dst=dst)
                if datapath.ports[out_port].is_tagged():
                    actions.append(parser.OFPActionPushVlan())
                    actions.append(parser.OFPActionSetField(vlan_vid=vid|ofp.OFPVID_PRESENT))
            actions.append(parser.OFPActionOutput(out_port))

            priority = datapath.config_default['high_priority']
            cookie = datapath.config_default['cookie']
            instructions = [parser.OFPInstructionActions(ofp.OFPIT_APPLY_ACTIONS, actions)]
            flowmod = parser.OFPFlowMod(
                datapath=None,
                cookie=cookie,
                priority=priority,
                match=match,
                instruction=instructions)
            ofmsgs.append(flowmod)
        return ofmsgs

    def datapath_connect(self, dp_id, ports):
        if dp_id != self.dp.dp_id:
            self.logger.error("Unknown dpid:%s", dp_id)
            return []
        else:
            datapath = self.dp

        for port in ports:
            # port numbers >= 0xF0000000 indicate logical ports
            if port > 0xF0000000:
                continue
            elif port not in datapath.ports:
                # Autoconfigure port
                self.logger.info(
                    "Autoconfiguring port:%s based on default config", port)
                datapath.add_port(port)
            datapath.ports[port].phys_up = True

        return self.send_config()
