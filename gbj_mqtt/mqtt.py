# -*- coding: utf-8 -*-
"""Module for communicating with MQTT brokers.

Notes
-----
- Connection to MQTT brokers is supposed to be over TCP.
- All parameters for MQTT brokers and clients should be defined
  in a configuration file utilized by a script or application employed this
  package.
- MQTT parameters are communicated with class instances of the module
  indirectly in form of pair option-section from the configuration file.
- Module constants are usual configuration options used at every form of MQTT
  brokers. Those options can be considered as common names for common
  identifiers either MQTT brokers or clients regardles of the configuration
  sections.
- Particular classed have their own class constants, which define specific
  configuration options and sections utilized for that form of MQTT broker.
- All module and class constants should be considered as default values.
  At each calling of respective methods specific configuration options and
  sections can be used. However, using in module standardized options and
  sections is recommended.

"""
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
OPTION_CLIENTID = 'clientid'
"""str: Configuration option with MQTT client identifier."""

OPTION_USERDATA = 'userdata'
"""str: Configuration option with custom data for MQTT callbacks."""

OPTION_HOST = 'host'
"""str: Configuration option with MQTT broker IP or URL."""

OPTION_PORT = 'port'
"""int: Configuration option with MQTT broker TCP port."""

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
    """Common MQTT management.

    Arguments
    ---------
    config : object
        Object for access to a configuration INI file.
        It is instance of the class ``Config`` from this package's module
        ``config``.
        Particular configuration file is already open.
        Injection of the config file object to this class instance is a form
        of attaching that file to this object.

    Notes
    -----
    This class should not be instanciated. It serves as a abstract class and
    a parent class for operational classes for particular MQTT brokers.

    See Also
    --------
    config.Config : Class for managing configuration INI files.

    """

    def __init__(self, config):
        """Create the class instance - constructor."""
        self._config = config
        self.connected = False  # Flag about connection to an MQTT broker
        # Logging
        self._logger = logging.getLogger(' '.join([__name__, __version__]))

    def __str__(self):
        """Represent instance object as a string."""
        msg = \
            f'ConfigFile(' \
            f'{self._config.configfile})'
        return msg

    def __repr__(self):
        """Represent instance object officially."""
        msg = f'{self.__class__.__name__}('
        if self._config:
            msg += f'config={repr(self._config.configfile)}'
        else:
            msg += f'None'
        return msg + ')'

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
    - The client utilizes MQTT topics and topic filters definitions from
      a configuration file.
    - The authorization of an MQTT client is supposed to be with username and
      password registered on connecting MQTT broker.
    - The encrypted communication (SSL/TSL) is not used.

    """

    GROUP_BROKER = 'MQTTbroker'
    """str: Predefined configuration section with MQTT broker parameters."""

    def __init__(self, config, **kwargs):
        """Create the class instance - constructor.

        Keyword Arguments
        -----------------

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
        super().__init__(config)
        # Client parameters
        self._clientid = self._config.option(
            OPTION_CLIENTID, self.GROUP_BROKER,
            socket.gethostname()
        )
        self._userdata = self._config.option(
            OPTION_USERDATA, self.GROUP_BROKER)
        self._userdata = kwargs.pop('userdata', self._userdata)
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
        if self._config:
            msg += f'config={repr(self._config.configfile)}'
        else:
            msg += f'None'
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

    def _on_connect(self, client, userdata, flags, rc):
        """Process actions when MQTT broker responds to a connection request.

        Arguments
        ---------
        client : object
            The client instance for this callback.
        userdata
            The private user data as set in Client() or user_data_set().
        flags : dict
            Response flags sent by the MQTT broker.
            ``flags['session present']`` is useful for clients that are
            using clean session set to `0` only. If a client with clean
            `session=0`, that reconnects to a broker that it has previously
            connected to, this flag indicates whether the broker still has the
            session information for the client. If `1`, the session still
            exists.
        rc : int
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
            self._connected = True
        if self._cb_on_connect is not None:
            self._cb_on_connect(client, RESULTS[rc], flags, rc)

    def _on_disconnect(self, client, userdata, rc):
        """Process actions when the client disconnects from the broker.

        Arguments
        ---------
        client : object
            The client instance for this callback.
        userdata
            The private user data as set in Client() or user_data_set().

        """
        self._logger.debug(f'MQTT disconnect result {rc}: {RESULTS[rc]}')
        if self._cb_on_disconnect is not None:
            self._cb_on_disconnect(client, RESULTS[rc], rc)
        self._client.loop_stop()
        self._connected = False

    def connect(self, username: str = None, password: str = None):
        """Connect to MQTT broker and set credentials.

        Arguments
        ---------
        username
            Login name of the registered user at MQTT broker.
        password
            Password of the registered user at MQTT broker.

        Raises
        -------
        SystemError
            Cannot connect to MQTT broker.

        """
        if not hasattr(self, '_client'):
            return
        # Broker parameters
        self._host = self._config.option(
            OPTION_HOST, self.GROUP_BROKER, 'localhost')
        self._port = int(self._config.option(
            OPTION_PORT, self.GROUP_BROKER, 1883))
        # Connect to broker
        msg = self._get_brokermsg('connection to')
        client = self._clientid
        user = username
        self._logger.info(f'{msg} as {client=} and {user=}')
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
