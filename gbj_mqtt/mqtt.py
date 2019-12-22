# -*- coding: utf-8 -*-
"""Module for communicating with MQTT brokers."""
__version__ = '0.1.0'
__status__ = 'Beta'
__author__ = 'Libor Gabaj'
__copyright__ = 'Copyright 2018-2019, ' + __author__
__credits__ = []
__license__ = 'MIT'
__maintainer__ = __author__
__email__ = 'libor.gabaj@gmail.com'


# Standard library modules
import time
import socket
import logging
from abc import ABC, abstractmethod
from enum import Enum

# Third party modules
import paho.mqtt.client as mqttclient
import paho.mqtt.publish as mqttpublish


###############################################################################
# Module parameters
###############################################################################
RESULTS = [
    'SUCCESS',
    'BAD PROTOCOL',
    'BAD CLIENT ID',
    'NO SERVER',
    'BAD CREDENTIALS',
    'NOT AUTHORISED',
]


class QoS(Enum):
    """Enumeration of possible MQTT quality of service levels."""
    AT_MOST_ONCE = 0
    AT_LEAST_ONCE = 1
    EXACTLY_ONCE = 2


###############################################################################
# Abstract class as a base for all MQTT clients
###############################################################################
class MQTT(ABC):
    """Common MQTT management."""

    def __init__(self):
        """Create the class instance - constructor."""
        self.connected = False  # Flag about connection to an MQTT broker
        # Logging
        self._logger = logging.getLogger(' '.join([__name__, __version__]))

    def __str__(self):
        """Represent instance object as a string."""
        return 'ConfigFile()'

    def __repr__(self):
        """Represent instance object officially."""
        msg = f'{self.__class__.__name__}()'
        return msg

    def check_qos(self, qos: QoS) -> int:
        """Check validity of the enumeration member and return its value.

        Returns
        -------
        int
            Quality of Service numeric code.

        Raises
        -------
        ValueError
            Input string is not an enumeration key.

        """
        try:
            if isinstance(qos, QoS):
                qos = qos.value
            else:
                qos = QoS[qos].value
            return qos
        except KeyError:
            errmsg = f'Unknown MQTT QoS {qos}'
            self._logger.error(errmsg)
            raise ValueError(errmsg)

    def check_topic(self, topic: str) -> str:
        """Check validity of the topic and return its value.

        Returns
        -------
        str
            String with MQTT topic.

        Raises
        -------
        ValueError
            Input is empty or not defined.

        """
        if not topic:
            errmsg = 'Empty MQTT topic'
            self._logger.error(errmsg)
            raise ValueError(errmsg)
        return str(topic)


###############################################################################
# Client of an MQTT broker
###############################################################################
class MqttBroker(MQTT):
    """Managing an MQTT client connection to usually local MQTT broker.

    Notes
    -----
    - The authorization of an MQTT client is supposed to be with username and
      password registered on connecting MQTT broker.
    - The encrypted communication (SSL/TSL) is not used.

    """

    def __init__(self, **kwargs):
        """Create the class instance - constructor.

        Keyword Arguments
        -----------------
        clientid : str
            MQTT client identifier.
        clean_session : boolean
            A flag that determines the client type. If 'True', the broker will
            remove all information about this client when it disconnects.
            If 'False', the client is a durable client and subscription
            information and queued messages will be retained when the client
            disconnects.
            Note that a client will never discard its own outgoing messages
            on disconnect. Calling 'connect()' or 'reconnect()' will cause
            the messages to be resent. Use 'reinitialise()' to reset a client
            to its original state.
        userdata
            User defined data of any type that is passed as the userdata
            parameter to callbacks. It may be updated at a later point with
            the 'user_data_set()' function.
        protocol : str
            The version of the MQTT protocol to use for this client. Can be
            either 'MQTTv31' or 'MQTTv311'.
        transport : str
            Set to 'websockets' to send MQTT over WebSockets. Leave at the
            default of 'tcp' to use raw TCP.
        connect : function
            Callback launched after connection to MQTT broker.
        disconnect : function
            Callback launched after disconnection from MQTT broker.
        subscribe : function
            Callback launched after subscription to MQTT topics.
        message : function
            Callback launched after receiving message from MQTT topics.

        Notes
        -----
        All keys for callback functions are root words from MQTT client
        callbacks without prefix ``on_``.

        """
        super().__init__()
        # Client parameters
        self._clientid = kwargs.pop('clientid', socket.gethostname())
        self._userdata = kwargs.pop('userdata', None)
        self._clean_session = bool(kwargs.pop('clean_session', True))
        self._protocol = kwargs.pop('protocol', mqttclient.MQTTv311)
        self._transport = kwargs.pop('transport', 'tcp')
        self._client = mqttclient.Client(
            self._clientid,
            self._clean_session,
            self._userdata,
            self._protocol,
            self._transport
            )
        # Callbacks definition
        self._cb_on_connect = kwargs.pop('connect', None)
        self._cb_on_disconnect = kwargs.pop('disconnect', None)
        self._cb_on_subscribe = kwargs.pop('subscribe', None)
        self._cb_on_message = kwargs.pop('message', None)
        # Callbacks
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        if self._cb_on_subscribe is not None:
            self._client.on_subscribe = self._cb_on_subscribe
        if self._cb_on_message is not None:
            self._client.on_message = self._cb_on_message
        # Logging
        self._logger.debug(
            'Instance of %s created: %s',
            self.__class__.__name__, str(self)
        )

    def __str__(self):
        """Represent instance object as a string."""
        msg = f'MQTTclient({self._clientid})'
        return msg

    def __repr__(self):
        """Represent instance object officially."""
        msg = f'{self.__class__.__name__}('
        msg += \
            f', clean_session={repr(self._clean_session)}' \
            f', userdata={repr(self._userdata)}' \
            f', protocol={repr(self._protocol)}' \
            f', transport={repr(self._transport)}'
        if self._cb_on_connect:
            msg += f', connect={self._cb_on_connect.__name__}'
        if self._cb_on_disconnect:
            msg += f', disconnect={self._cb_on_disconnect.__name__}'
        if self._cb_on_subscribe:
            msg += f', subscribe={self._cb_on_subscribe.__name__}'
        if self._cb_on_message:
            msg += f', message={self._cb_on_message.__name__}'
        msg += f')'
        return msg

    def _get_brokermsg(self, action: str) -> str:
        msg = f'MQTT {action} broker "{self._host}:{self._port}"'
        return msg

    def _on_connect(self,
                    client: mqttclient,
                    userdata: any,
                    flags: dict(),
                    rc: int):
        """Process actions when MQTT broker responds to a connection request.

        Arguments
        ---------
        client
            The client instance for this callback.
        userdata
            The private user data as set in Client() or user_data_set().
        flags
            Response flags sent by the MQTT broker.
            ``flags['session present']`` is useful for clients that are
            using clean session set to `0` only. If a client with clean
            `session=0`, that reconnects to a broker that it has previously
            connected to, this flag indicates whether the broker still has the
            session information for the client. If `1`, the session still
            exists.
        rc
            The connection result (result code):

            - 0: Connection successful
            - 1: Connection refused - incorrect protocol version
            - 2: Connection refused - invalid client identifier
            - 3: Connection refused - server unavailable
            - 4: Connection refused - bad username or password
            - 5: Connection refused - not authorised
            - 6 ~ 255: Currently unused

        See Also
        --------
        Client(),  user_data_set() : Methods from imported module.

        """
        self._wating = False
        self._logger.debug(f'MQTT connect result {rc=}: {RESULTS[rc]}')
        if rc == 0:
            self.connected = True
        if self._cb_on_connect is not None:
            self._cb_on_connect(client, RESULTS[rc], flags, rc)

    def _on_disconnect(self,
                       client: mqttclient,
                       userdata: any,
                       rc: int):
        """Process actions when the client disconnects from the broker.

        Arguments
        ---------
        client
            The client instance for this callback.
        userdata
            The private user data as set in Client() or user_data_set().
        rc
            The connection result (result code).

        """
        self._logger.debug(f'MQTT disconnect result {rc}: {RESULTS[rc]}')
        if self._cb_on_disconnect is not None:
            self._cb_on_disconnect(client, RESULTS[rc], rc)
        self._client.loop_stop()
        self.connected = False

    def connect(self, **kwargs):
        """Connect to MQTT broker and set credentials.

        Keyword Arguments
        -----------------
        username : str
            Login name of the registered user at MQTT broker.
        password : str
            Password of the registered user at MQTT broker.
        host : str
            MQTT broker IP or URL.
        port : int
            MQTT broker TCP port.

        Raises
        -------
        SystemError
            Cannot connect to MQTT broker.

        """
        if not hasattr(self, '_client'):
            return
        # Broker parameters
        self._host = kwargs.pop('host', 'localhost')
        self._port = int(kwargs.pop('port', 1883))
        # Connect to broker
        username = kwargs.pop('username')
        password = kwargs.pop('password')
        msg = self._get_brokermsg('connection to')
        client = self._clientid
        self._logger.info(f'{msg} as {client=} and {username=}')
        self._wating = True
        try:
            self._client.loop_start()
            if username is not None:
                self._client.username_pw_set(username, password)
            self._client.connect(self._host, self._port)
        except Exception as errmsg:
            errmsg = f'{msg} failed: {errmsg}'
            self._client.loop_stop()
            self._logger.error(errmsg)
            raise SystemError(errmsg)
        # Waiting for connection
        while self._wating:
            time.sleep(0.2)

    def disconnect(self):
        """Disconnect from MQTT broker.

        Raises
        -------
        SystemError
            Cannot disconnect from MQTT broker.

        """
        if not hasattr(self, '_client'):
            return
        msg = self._get_brokermsg('disconnection from')
        client = self._clientid
        self._logger.info(f'{msg} as {client=}')
        try:
            self._client.loop_stop()
            self._client.disconnect()
        except Exception as errmsg:
            errmsg = f'{msg} failed: {errmsg}'
            self._logger.error(errmsg)
            raise SystemError(errmsg)

    def reconnect(self):
        """Reconnect to MQTT broker.

        Raises
        -------
        SystemError
            Cannot reconnect to MQTT broker.

        """
        if not hasattr(self, '_client'):
            return
        msg = self._get_brokermsg('reconnection to')
        client = self._clientid
        self._logger.info(f'{msg} as {client=}')
        self._wating = True
        try:
            self._client.reconnect()
        except Exception as errmsg:
            errmsg = f'{msg} failed: {errmsg}'
            self._logger.error(errmsg)
            raise SystemError(errmsg)
        # Waiting for connection
        while self._wating:
            time.sleep(0.2)

    def subscribe(self,
                  topic: str,
                  qos: QoS = QoS.AT_MOST_ONCE):
        """Subscribe to an MQTT topic.

        Arguments
        ---------
        topic
            MQTT topic, which should be subscribed to.
        qos
            Quality of Service enumeration item or directly its value.

        Raises
        -------
        SystemError
            Cannot subscribe to MQTT topic.

        """
        if not self.connected:
            return
        topic = self.check_topic(topic)
        qos = self.check_qos(qos)
        result = self._client.subscribe(topic, qos)
        rc = result[0]
        if rc == mqttclient.MQTT_ERR_SUCCESS:
            self._logger.debug(f'MQTT client subscribed to {topic=}, {qos=}')
        # elif rc == mqttclient.MQTT_ERR_NO_CONN:
        else:
            errmsg = \
                f'MQTT subscription to {topic=}' \
                f' failed with error code {rc=}'
            self._logger.error(errmsg)
            raise SystemError(errmsg)

    def publish(self,
                message: str,
                topic: str,
                qos: QoS = QoS.AT_MOST_ONCE,
                retain: bool = False):
        """Publish to an MQTT topic.

        Arguments
        ---------
        message
            Data to be published into the topic.
        topic
            MQTT topic, which should be published to.
        qos
            Quality of Service enumeration item or directly its value.
        retain
            Flag about retaining a message on the MQTT broker.

        Raises
        -------
        SystemError
            Cannot publish to a MQTT topic.

        """
        if not self.connected:
            return
        topic = self.check_topic(topic)
        qos = self.check_qos(qos)
        retain = bool(retain)
        msg = f'Publishing to MQTT {topic=}'
        try:
            self._client.publish(topic, message, qos, retain)
            msg = f'{msg}, {qos=}, {retain=}: {message}'
            self._logger.debug(msg)
        except Exception as errmsg:
            errmsg = f'{msg} failed: {errmsg}'
            self._logger.error(errmsg)
            raise SystemError(errmsg)

    def lwt(self,
            message: str,
            topic: str,
            qos: QoS = QoS.AT_MOST_ONCE,
            retain: bool = True):
        """Set last will and testament.

        Arguments
        ---------
        message
            Data to be set as LWT payload.
        topic
            MQTT topic, which should be LWT published to.
        qos
            Quality of Service enumeration item or directly its value.
        retain
            Flag about retaining a message on the MQTT broker.

        Raises
        -------
        SystemError
            Cannot set LWT for a MQTT client.

        """
        if not hasattr(self, '_client'):
            return
        topic = self.check_topic(topic)
        qos = self.check_qos(qos)
        retain = bool(retain)
        msg = f'MQTT LWT {topic=}'
        try:
            self._client.will_set(topic, message, qos, retain)
            msg = f'{msg}, {qos=}, {retain=}: {message}'
            self._logger.debug(msg)
        except Exception as errmsg:
            errmsg = f'{msg} failed: {errmsg}'
            self._logger.error(errmsg)
            raise SystemError(errmsg)
