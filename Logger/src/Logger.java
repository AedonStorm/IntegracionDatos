import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.json.JSONArray;
import org.json.JSONException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class Logger {
	private final static String NOMBRE_COLA_NOMINAL = "nominal";

	public static void main(String [ ] args) throws IOException, TimeoutException
	{
		ConnectionFactory factory = new ConnectionFactory();
	      factory.setHost("localhost");
	      Connection connection = factory.newConnection();
	      
	      Channel channel = connection.createChannel();
	      
	      channel.queueDeclare(NOMBRE_COLA_NOMINAL, false, false, false, null);
	      
	      Consumer consumer = new DefaultConsumer(channel) {
	    	
	    	  @Override
	    	  public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
	    		 String message = new String(body, "UTF-8");
	    		 JSONArray obj;
				try {
					obj = new JSONArray(message);
					LogReader(obj);
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	    	  }
	      };
	      channel.basicConsume(NOMBRE_COLA_NOMINAL, true, consumer);
	}
	
	private static void LogReader(JSONArray jsonList) {
		//System.out.println(json.toString());
		for (int i = 0; i < jsonList.length(); i++) {
			try {
				System.out.println("Tipo: " + jsonList.getJSONObject(i).get("tipo").toString());
				System.out.println("	Punto: " + jsonList.getJSONObject(i).get("punto").toString());
				System.out.println("	Latitud: " + jsonList.getJSONObject(i).get("latitud").toString());
				System.out.println("	Longitud: " + jsonList.getJSONObject(i).get("longitud").toString());
				System.out.println("	Descripción: " + jsonList.getJSONObject(i).get("descripcion").toString());
				System.out.println("___________________________________________________________");
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
	}
}
