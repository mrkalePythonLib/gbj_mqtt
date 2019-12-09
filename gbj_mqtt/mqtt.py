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
__version__ = '0.4.0'
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
import abc
# Third party modules
import paho.mqtt.client as mqttclient
import paho.mqtt.publish as mqttpublish


###############################################################################
# Module constants
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


###############################################################################
# Abstract class as a base for all MQTT clients
###############################################################################
class MQTT(abc.ABC):
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
        self._connected = False
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

    @property
    def connected(self):
        """Flag about successful connection to an MQTT broker."""
        return self._connected


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

    # Predefined configuration file sections related to MQTT
    GROUP_BROKER = 'MQTTbroker'
    """str: Predefined configuration section with MQTT broker parameters."""

    GROUP_TOPICS = 'MQTTtopics'
    """str: Predefined configuration section with MQTT topics."""

    GROUP_FILTERS = 'MQTTfilters'
    """str: Predefined configuration section with MQTT topic filters."""

    GROUP_DEFAULT = 'DEFAULT'
    """str: Default configuration section with MQTT variables."""

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
        msg = \
            f'MQTTclient(' \
            f'{self._clientid})'
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

    def topic_def(self, option, section=GROUP_TOPICS):
        """Return MQTT topic definition parameters.

        Arguments
        ---------
        option : str
            Configuration option from attached configuration file with
            definition of an MQTT topic, which should be read.
            *The argument is mandatory and has no default value.*
        section : str
            Configuration section from attached configuration file, where
            configuration option should be searched.

        Returns
        -------
        tuple of str and boolean
            MQTT topic parameters as ``name``, ``qos``, ``retain``.

        Notes
        -----
        The method appends ``0`` as the default `qos` and ``0`` as default
        ``retain`` to the read topic definition for cases, when no `qos` and
        `retain` is defined in order to split the topic properly.

        """
        try:
            params = self._config.option_split(option, section, ['0', '0'])
            name = params[0]
            qos = abs(int(params[1]))
            retain = bool(abs(int(params[2])))
        except TypeError:
            name = None
            qos = None
            retain = None
        return (name, qos, retain)

    def topic_name(self, option, section=GROUP_TOPICS):
        """Return MQTT topic name.

        Arguments
        ---------
        option : str
            Configuration option from attached configuration file with
            definition of an MQTT topic, which should be read.
            *The argument is mandatory and has no default value.*
        section : str
            Configuration section from attached configuration file, where
            configuration option should be searched.

        Returns
        -------
        str
            MQTT topic name.

        """
        params = self.topic_def(option, section)
        return params[0]

    def topic_qos(self, option, section=GROUP_TOPICS):
        """Return MQTT topic QoS.

        Arguments
        ---------
        option : str
            Configuration option from attached configuration file with
            definition of an MQTT topic, which should be read.
            *The argument is mandatory and has no default value.*
        section : str
            Configuration section from attached configuration file, where
            configuration option should be searched.

        Returns
        -------
        int
            MQTT topic `quality of service` as an absolute integer of second
            parameter of read topic definition.

        """
        params = self.topic_def(option, section)
        return params[1]

    def topic_retain(self, option, section=GROUP_TOPICS):
        """Return MQTT topic retain flag.

        Arguments
        ---------
        option : str
            Configuration option from attached configuration file with
            definition of an MQTT topic, which should be read.
            *The argument is mandatory and has no default value.*
        section : str
            Configuration section from attached configuration file, where
            configuration option should be searched.

        Returns
        -------
        boolean
            MQTT topic `retain` as an logical value of third parameter of read
            topic definition.

        """
        params = self.topic_def(option, section)
        return params[2]

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
        self._logger.debug('MQTT connect result %s: %s', rc, RESULTS[rc])
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
        self._logger.debug('MQTT disconnect result %s: %s', rc, RESULTS[rc])
        if self._cb_on_disconnect is not None:
            self._cb_on_disconnect(client, RESULTS[rc], rc)
        self._client.loop_stop()
        self._connected = False

    def callback_filters(self, **kwargs):
        """Register callback functions for particular MQTT topic groups.

        Keyword Arguments
        -----------------
        key : str
            The key of the argument dictionary is a configuration option
            defining a topic, to which a topic filter is related.
        value : tuple
            The value of the argument dictionary is a tuple with ``callback``
            and ``section`` defining MQTT topic filter.

            - If section is `GROUP_FILTERS`, the argument value can be just
              the callback function.
            - If section is not `GROUP_FILTERS`, the argument value should be
              a tuple:

                server_test=mqtt_on_message_test
                server_sensor_temp=(mqtt_on_temp, ConfigMqtt.GROUP_TOPICS)

        Notes
        -----
        - The method should be used before subscribing to respective filtered
          topics. The best place is in the ``on_connect`` callback.
        - The method can be called multiple times. For configuration option
          used at a previous call the filter callback is just updated.
        - If the callback is None, the filter for corresponding topic is
          removed.

        """
        for option in kwargs:
            definition = kwargs[option]
            if isinstance(definition, tuple):
                callback = definition[0]
                section = definition[1]
            else:
                callback = definition
                section = self.GROUP_FILTERS
            topic = self.topic_name(option, section)
            self._logger.debug(
                'MQTT filter callback %s for topic %s',
                callback.__name__, topic)
            if topic is not None:
                if callback is None:
                    self._client.message_callback_remove(topic)
                else:
                    self._client.message_callback_add(topic, callback)

    def connect(self, username=None, password=None):
        """Connect to MQTT broker and set credentials.

        Arguments
        ---------
        username : str
            Login name of the registered user at MQTT broker.
        password : str
            Password of the registered user at MQTT broker.

        """
        if not hasattr(self, '_client'):
            return
        # Broker parameters
        self._host = self._config.option(
            OPTION_HOST, self.GROUP_BROKER, 'localhost')
        self._port = int(self._config.option(
            OPTION_PORT, self.GROUP_BROKER, 1883))
        # Connect to broker
        self._logger.info(
            'MQTT connection to broker %s:%s as client %s and user %s',
            self._host, self._port, self._clientid, username)
        self._wating = True
        try:
            self._client.loop_start()
            if username is not None:
                self._client.username_pw_set(username, password)
            self._client.connect(self._host, self._port)
        except Exception as errmsg:
            self._client.loop_stop()
            self._logger.error(
                'MQTT connection to %s:%s failed: %s',
                self._host, self._port, errmsg,  # exc_info=True
                )
            raise Exception(errmsg)
        # Waiting for connection
        while self._wating:
            time.sleep(0.2)

    def disconnect(self):
        """Disconnect from MQTT broker."""
        if not hasattr(self, '_client'):
            return
        # Disconnect from broker
        self._logger.info(
            'MQTT disconnection from broker %s:%s as client %s',
            self._host, self._port, self._clientid)
        try:
            self._client.loop_stop()
            self._client.disconnect()
        except Exception as errmsg:
            self._logger.error(
                'MQTT disconnection from %s:%s failed: %s',
                self._host, self._port, errmsg,  # exc_info=True
                )
            raise Exception(errmsg)

    def reconnect(self):
        """Reconnect to MQTT broker."""
        if not hasattr(self, '_client'):
            return
        # Reconnect to broker
        self._logger.info(
            'MQTT reconnection to broker %s:%s as client %s',
            self._host, self._port, self._clientid)
        self._wating = True
        try:
            self._client.reconnect()
        except Exception as errmsg:
            self._logger.error(
                'MQTT reconnection to %s:%s failed: %s',
                self._host, self._port, errmsg,  # exc_info=True
                )
            raise Exception(errmsg)
        # Waiting for connection
        while self._wating:
            time.sleep(0.2)

    def subscribe_filters(self):
        """Subscribe to all MQTT topic filters.

        Raises
        -------
        Exception
            General exception with error code.

        """
        if not self.connected:
            return
        for option in self._config.options(self.GROUP_FILTERS):
            topic, qos, _ = self.topic_def(option, self.GROUP_FILTERS)
            result = self._client.subscribe(topic, qos)
            if result[0] == mqttclient.MQTT_ERR_SUCCESS:
                self._logger.debug(
                    'MQTT subscribe to filter %s, %s',
                    topic, qos)
            # elif result[0] == mqttclient.MQTT_ERR_NO_CONN:
            else:
                self._logger.error(
                    'MQTT filter subscribe result %s',
                    result[0])
                raise Exception(str(result[0]))

    def subscribe_topic(self, option, section=GROUP_TOPICS):
        """Subscribe to an MQTT topic.

        Arguments
        ---------
        option : str
            Configuration option from attached configuration file with
            definition of an MQTT topic, which should be read.
            *The argument is mandatory and has no default value.*
        section : str
            Configuration section from attached configuration file, where
            configuration option should be searched.

        Raises
        -------
        Exception
            General exception with error code.

        """
        if not self.connected:
            return
        topic, qos, _ = self.topic_def(option, self.GROUP_TOPICS)
        result = self._client.subscribe(topic, qos)
        if result[0] == mqttclient.MQTT_ERR_SUCCESS:
            self._logger.debug(
                'MQTT subscribe to topic %s, %d',
                topic, qos)
        # elif result[0] == mqttclient.MQTT_ERR_NO_CONN:
        else:
            self._logger.error(
                'MQTT topic subscribe result %d',
                result[0])
            raise Exception(str(result[0]))

    def publish(self, message, option, section=GROUP_TOPICS):
        """Publish to an MQTT topic.

        Arguments
        ---------
        message : str
            Data to be published into the topic.
            *The argument is mandatory and has no default value.*
        option : str
            Configuration option from attached configuration file with
            definition of an MQTT topic, which should be published to.
            *The argument is mandatory and has no default value.*
        section : str
            Configuration section from attached configuration file, where
            configuration option should be searched.

        Raises
        -------
        Exception
            General exception with error code.

        """
        if not self.connected:
            return
        topic, qos, retain = self.topic_def(option, section)
        if topic is not None:
            self._client.publish(topic, message, qos, retain)
            self._logger.debug(
                'MQTT publishing to topic %s, %d, %s: %s',
                topic, qos, retain, message)
        else:
            self._logger.error(
                'Publishing to MQTT topic option %s:[%s] failed',
                option, section)
            raise Exception('Unknown option or section')

    def lwt(self, message, option, section=GROUP_TOPICS):
        """Set last will and testament.

        Arguments
        ---------
        message : str
            Data to be set as LWT payload.
            *The argument is mandatory and has no default value.*
        option : str
            Configuration option from attached configuration file with
            definition of an MQTT topic for LWT.
            *The argument is mandatory and has no default value.*
        section : str
            Configuration section from attached configuration file, where
            configuration option should be searched.

        Raises
        -------
        Exception
            If qos is not 0, 1 or 2, or if topic is None
            or has zero string length.

        """
        if not hasattr(self, '_client'):
            return
        topic, qos, retain = self.topic_def(option, section)
        try:
            self._client.will_set(topic, message, qos, retain)
            self._logger.debug(
                'MQTT LWT to topic %s, %d, %s: %s',
                topic, qos, retain, message)
        except ValueError:
            self._logger.error(
                'LWT to MQTT topic option %s:[%s] failed',
                option, section)
            raise Exception('Unknown option, section, or topic parameters')


###############################################################################
# Client of ThingSpeak cloud
###############################################################################
class ThingSpeak(MQTT):
    """Connect and publish to ThingSpeak MQTT broker.

    Arguments
    ---------
    config : object
        Object for access to a configuration INI file.
        It is instance of the class ``Config`` from this package module
        ``config``.
        Particular configuration is already open.
        Injection of the config file object to this class instance it a form
        of attaching that file to this object.

    Raises
    -------
    TypeError
        Configuration parameters without value in configuration file.

    Notes
    -----
    - The class only provides single publishing to ThingSpeak, so that
      connecting and disconnecting to the ThingSpeak broker is automatic.
    - The class follows allowed delay between publishings set for free acounts
      and buffers frequent messagges preferably with status.
    - The reading from ThingSpeak channels is not implemented.

    See Also
    --------
    config.Config : Class for managing configuration INI files.

    """

    # Predefined configuration file sections and options related to ThingSpeak
    GROUP_BROKER = 'ThingSpeak'
    """str: Configuration section with ThingSpeak parameters."""

    OPTION_MQTT_API_KEY = 'mqtt_api_key'
    """str: Configuration option with ThingSpeak MQTT API key."""

    OPTION_CHANNEL_ID = 'channel_id'
    """int: Configuration option with ThingSpeak channel id."""

    OPTION_WRITE_API_KEY = 'write_api_key'
    """str: Configuration option with ThingSpeak write key."""

    PUBLISH_DELAY_MIN = 15.0
    """float: Minimal allowed publish delay in seconds."""

    FIELD_MIN = 1
    """int: Minimal channel field number."""

    FIELD_MAX = 8
    """int: Maximal channel field number."""

    def __init__(self, config):
        """Create the class instance - constructor."""
        super().__init__(config)
        self._timestamp_publish_last = 0.0
        # Defaulted configuration parameters
        self._clientid = self._config.option(
            OPTION_CLIENTID, self.GROUP_BROKER, socket.gethostname())
        self._port = int(self._config.option(
            OPTION_PORT, self.GROUP_BROKER, 1883))
        self._host = self._config.option(
            OPTION_HOST, self.GROUP_BROKER, 'mqtt.thingspeak.com')
        # Configuration parameters without default value
        errtxt = 'Undefined ThingSpeak config option {}'
        #
        self._mqtt_api_key = self._config.option(
            self.OPTION_MQTT_API_KEY, self.GROUP_BROKER)
        if self._mqtt_api_key is None:
            errmsg = errtxt.format(self.OPTION_MQTT_API_KEY)
            self._logger.error(errmsg)
            raise TypeError(errmsg)
        #
        self._channel_id = self._config.option(
            self.OPTION_CHANNEL_ID, self.GROUP_BROKER)
        if self._channel_id is None:
            errmsg = errtxt.format(self.OPTION_CHANNEL_ID)
            self._logger.error(errmsg)
            raise TypeError(errmsg)
        #
        self._write_api_key = self._config.option(
            self.OPTION_WRITE_API_KEY, self.GROUP_BROKER)
        if self._write_api_key is None:
            errmsg = errtxt.format(self.OPTION_WRITE_API_KEY)
            self._logger.error(errmsg)
            raise TypeError(errmsg)
        # Initialize data buffer
        self._buffer = {}
        self.reset()
        self._logger.debug(
            'Instance of %s created: %s',
            self.__class__.__name__, str(self)
        )

    def __str__(self):
        """Represent instance object as a string."""
        msg = \
            f'ThingSpeakClient(' \
            f'{self._host}:{self._port}/{self._clientid})'
        return msg

    def _fieldname(self, field_num):
        """Construct field name from field number."""
        if field_num in range(self.FIELD_MIN, self.FIELD_MAX + 1):
            field_name = 'field{}'.format(field_num)
        else:
            field_name = None
        return field_name

    def store_field(self, field_num, field_value=None):
        """Store value to a channel field or reset it.

        Arguments
        ---------
        field_num : int
            Number of a field, which a values is targeted to.
            If it is not in expected range, nothing is stored and false flag
            is returned.
        field_value : float
            Value to be published in the field with provided number.
            If not provided the value is reset.

        """
        field_name = self._fieldname(field_num)
        if field_name is not None:
            self._buffer[field_name] = field_value
            self._logger.debug(
                'Buffered ThingSpeak field%d with value %s',
                field_num, field_value)

    def store_status(self, status=None):
        """Store status to a channel status or reset it.

        Arguments
        ---------
        status : str | float | int
            Status value to be published in the status.
            If not provided the value is reset.

        """
        self._buffer['status'] = status
        self._logger.debug(
            'Buffered ThingSpeak status %s',
            status)

    def reset(self):
        """Reset all buffered fields and status."""
        self._buffer['status'] = None
        for field in range(self.FIELD_MIN, self.FIELD_MAX + 1):
            self._buffer[self._fieldname(field)] = None
        self._logger.debug('Reset all ThingSpeak data')

    def publish(self, *arg, **kwargs):
        """Publish single message to ThingSpeak with buffered values.

        Arguments
        ---------
        Unused arguments just in case the method is called directly from
        a timer.

        Keyword Arguments
        -----------------
        Unused arguments just in case the method is called directly from
        a timer.

        Notes
        -----
        - All buffered fields with values other than None are published.
        - Buffered status is published, if its value is other than None.
        - If publishing is earlier than minimal allowed publishing period,
          the publishing is ignored.

        Returns
        -------
        bool
            Flag about real, successful publishing to ThingSpeak.

        Raises
        ------
        Exception
            General exception with error message from ThingSpeak.

        """
        # Check publishing period
        if (time.time() - self._timestamp_publish_last) \
           < self.PUBLISH_DELAY_MIN:
            self._logger.warning('Ignored frequent publishing to ThingSpeak')
            return False
        # Construct message payload
        msgParts = []
        for field_num in range(self.FIELD_MIN, self.FIELD_MAX + 1):
            field_name = self._fieldname(field_num)
            field_value = self._buffer[field_name]
            if field_value is None:
                continue
            msgParts.append('{}={}'.format(field_name, field_value))
        if self._buffer['status'] is not None:
            msgParts.append('status={}'.format(self._buffer['status']))
        msgPayload = '&'.join(msgParts)
        # Publish payload
        if msgPayload:
            # Construct topic
            topicParts = [
                'channels', self._channel_id,
                'publish', self._write_api_key
            ]
            topic = '/'.join(topicParts)
            try:
                self._logger.debug('Publishing to ThingSpeak channel %s',
                                   self._channel_id)
                mqttpublish.single(
                    topic,
                    payload=msgPayload,
                    hostname=self._host,
                    port=self._port,
                    auth={'username': self._clientid,
                          'password': self._mqtt_api_key,
                          }
                )
                self._timestamp_publish_last = time.time()
                self._logger.debug(
                    'Published ThingSpeak message %s',
                    msgPayload
                )
                return True
            except Exception as errmsg:
                self._logger.error(
                    'Publishing to ThingSpeak failed with error %s:',
                    errmsg,   # exc_info=True
                    )
        else:
            self._logger.debug('Nothing published to ThingSpeak')
        return False
