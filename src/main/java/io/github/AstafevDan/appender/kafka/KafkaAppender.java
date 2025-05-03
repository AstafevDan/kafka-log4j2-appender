package io.github.AstafevDan.appender.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.core.*;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;

import java.io.Serializable;
import java.util.Properties;

/**
 * Кастомный Log4j2 Appender для отправки логов в топик Kafka.
 *
 * @author Даниил Астафьев
 * @version 1.0
 */
@Plugin(
        name = "KafkaAppender",
        category = Core.CATEGORY_NAME,
        elementType = Appender.ELEMENT_TYPE
)
public class KafkaAppender extends AbstractAppender {

    private Producer<String, String> producer;
    private String topic;
    private String bootstrapServers;

    /**
     * Конструктор аппендера.
     *
     * @param name             Уникальное имя аппендера
     * @param topic            Kafka-топик для отправки логов
     * @param bootstrapServers Список брокеров Kafka
     * @param layout           Формат логов (например, PatternLayout)
     */
    protected KafkaAppender(String name, String topic, String bootstrapServers, Layout<? extends Serializable> layout) {
        super(name, null, layout, false, null);
        this.topic = topic;
        this.bootstrapServers = bootstrapServers;
        initKafkaProducer();
    }

    /**
     * Инициализация Kafka Producer с настройками.
     */
    private void initKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(props);
    }

    /**
     * Фабричный метод для создания аппендера.
     *
     * @param name             Имя аппендера
     * @param topic            Топик Kafka
     * @param bootstrapServers Адреса брокеров
     * @param layout           Формат сообщений
     * @return Экземпляр KafkaAppender
     */
    @PluginFactory
    public static KafkaAppender createAppender(
            @PluginAttribute("name") String name,
            @PluginAttribute("topic") String topic,
            @PluginAttribute("bootstrapServers") String bootstrapServers,
            @PluginElement("Layout") Layout<? extends Serializable> layout
    ) {
        return new KafkaAppender(name, topic, bootstrapServers, layout);
    }

    /**
     * Отправка логов в топик Kafka.
     *
     * @param event Событие лога
     */
    @Override
    public void append(LogEvent event) {
        String message = new String(getLayout().toByteArray(event));
        producer.send(new ProducerRecord<>(topic, message), (metadata, exception) -> {
            if (exception != null) {
                System.err.println("Error sending message to Kafka: " + exception.getMessage());
            }
        });
    }

    /**
     * Остановка аппендера с закрытием Kafka Producer.
     */
    @Override
    public void stop() {
        super.stop();
        if (producer != null) {
            producer.close();
        }
    }
}
