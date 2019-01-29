import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Node;
import org.json.JSONArray;
import org.json.JSONObject;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

//Bicis
public class Transformador {
	private final static String NOMBRE_EXCHANGE = "bicis";
	private final static String DATA_TYPE = "xml";
	private final static String NOMBRE_COLA_NOMINAL = "nominal";

	public static void main(String [ ] args) throws IOException, TimeoutException
	{
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();
		
		channel.exchangeDeclare(NOMBRE_EXCHANGE, BuiltinExchangeType.TOPIC);
		String COLA_CONSUMER = channel.queueDeclare().getQueue();
		channel.queueBind(COLA_CONSUMER, NOMBRE_EXCHANGE, DATA_TYPE);
		
		channel.queueDeclare(NOMBRE_COLA_NOMINAL, false, false, false, null);
      
		Consumer consumer = new DefaultConsumer(channel) {
    	  @Override
    	  public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
    		 String message = new String(body, "UTF-8");
    		 //System.out.println("Recibido: '"+ message +"'");
    		 JSONArray json = dataTransform(message);
   	      	 channel.basicPublish("", NOMBRE_COLA_NOMINAL, null, json.toString().getBytes());
    	  }
		};
		channel.basicConsume(COLA_CONSUMER, true, consumer);
	}
	
	private static JSONArray dataTransform(String data) {
		JSONArray jsonList;
		try {
			jsonList = new JSONArray();
			Document document = DocumentHelper.parseText(data);
			List<? extends Node> nodeList = document.selectNodes("//*[local-name() = 'Placemark']");
			
			for (Node placemark: nodeList) {
				JSONObject json = new JSONObject();
				
				Node name = placemark.selectSingleNode(".//*[@name='address']");
				String punto = name.selectSingleNode(".//*[local-name() = 'value']").getText();
				json.put("punto", punto);
				
				json.put("tipo", "Estaciones ValenBici");
				
				//Node point = placemark.selectSingleNode(".//*[]");
				String[] coordinates = placemark.selectSingleNode(".//*[local-name() = 'coordinates']").getText().split(",");
				json.put("latitud", coordinates[0]);
				json.put("longitud", coordinates[1]);
				
				Node available = placemark.selectSingleNode(".//*[@name='available']");
				String numeroBicis = available.selectSingleNode(".//*[local-name() = 'value']").getText();
				
				Node free = placemark.selectSingleNode(".//*[@name='free']");
				String numeroPlazasVacias = free.selectSingleNode(".//*[local-name() = 'value']").getText();
				
				json.put("descripcion", "La estacion tiene " + numeroBicis + " disponibles y " 
				+ numeroPlazasVacias + " espacios para dejar la bici.");
				
				jsonList.put(json);
			}
			return jsonList;
		} catch (Exception e ) {e.printStackTrace(); return null;}
	}
}
