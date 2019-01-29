import java.io.IOException;
import java.util.concurrent.TimeoutException;

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

//Trafico
public class Transformador {
	private final static String NOMBRE_EXCHANGE = "trafico";
	private final static String DATA_TYPE = "json";
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
			JSONObject obj = new JSONObject(data);
			JSONArray jsonArrayFeatures = obj.getJSONArray("features");
			
			for (int i = 0; i < jsonArrayFeatures.length(); i++) {
				JSONObject json = new JSONObject();
				JSONObject properties = jsonArrayFeatures.getJSONObject(i).getJSONObject("properties");
				JSONObject geometry = jsonArrayFeatures.getJSONObject(i).getJSONObject("geometry");
				
				json.put("punto", properties.get("denominacion"));
				
				json.put("tipo", "Datos de tráfico");
				JSONArray coordinates = geometry.getJSONArray("coordinates");
				json.put("latitud", coordinates.getJSONArray(0).get(0).toString());
				json.put("longitud", coordinates.getJSONArray(0).get(1).toString());
				
				String estado = "";
				switch(properties.get("estado").toString()) {
					case "0": estado = "fluido"; break;
					case "1": estado = "denso"; break;
					case "2": estado = "congestionado"; break;
					case "3": estado = "cortado"; break;
					default: estado = "desconocido";
				}
				json.put("descripcion","Estado del trafico es " + estado);
				
				jsonList.put(json);
			}
			return jsonList;
		} catch (Exception e ) {e.printStackTrace(); return null;}
	}
}
