package pe.edu.upeu.ec.pago.configuration;

import pe.edu.upeu.ec.pago.event.EventoOrden;
import pe.edu.upeu.ec.pago.event.EventoPago;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

@EnableKafka
@Configuration
public class KafkaConfiguracion {

    private static final Logger log = LoggerFactory.getLogger(KafkaConfiguracion.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ProducerFactory<String, EventoPago> producerFactory() {
        Map<String, Object> propiedades = new HashMap<>();
        propiedades.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        propiedades.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        propiedades.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        return new DefaultKafkaProducerFactory<>(propiedades);
    }

    @Bean
    public KafkaTemplate<String, EventoPago> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public ConsumerFactory<String, EventoOrden> consumerFactory() {
        Map<String, Object> propiedades = new HashMap<>();
        propiedades.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        propiedades.put(ConsumerConfig.GROUP_ID_CONFIG, "ec-pago-ms-group");
        propiedades.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        propiedades.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        propiedades.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        propiedades.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        propiedades.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        propiedades.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        propiedades.put(JsonDeserializer.VALUE_DEFAULT_TYPE, EventoOrden.class.getName());
        propiedades.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

        return new DefaultKafkaConsumerFactory<>(
                propiedades,
                new ErrorHandlingDeserializer<>(new StringDeserializer()),
                new ErrorHandlingDeserializer<>(new JsonDeserializer<>())
        );
    }

    @Bean
    public CommonErrorHandler kafkaErrorHandler() {
        DefaultErrorHandler errorHandler = new DefaultErrorHandler((record, exception) ->
                log.error(
                        "Mensaje descartado de Kafka en topic={}, particion={}, offset={}: {}",
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        exception.getMessage(),
                        exception
                ),
                new FixedBackOff(0L, 0L)
        );
        errorHandler.setCommitRecovered(true);
        return errorHandler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, EventoOrden> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, EventoOrden> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setCommonErrorHandler(kafkaErrorHandler());
        return factory;
    }
}
