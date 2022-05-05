using System;
using System.Collections.Generic;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitMQ.Example
{
    internal class Program
    {
        //private static readonly string _queueName = "AYKUTGURSEL";
        private static Publisher _publisher;
        private static Consumer _consumer;

        static void Main(string[] args)
        {
            List<string> queueNameList = new List<string>
            {
                "MailSender",
                "SendSMS",
                "SendInvoice",
                "SendExcel",
                "GeneratePDF"
            };


            for (int i = 0; i < 11; i++)
            {
                foreach (var queueName in queueNameList)
                {
                    _publisher = new Publisher(queueName, "Aykut Gürsel");
                }
            }

            foreach (var queueName in queueNameList)
            {
                _consumer = new Consumer(queueName);
            }


            Console.ReadKey();
        }


        public class RabbitMQService
        {
            // localhost üzerinde kurulu olduğu için host adresi olarak bunu kullanıyorum.
            private readonly string _hostName = "localhost";

            public IConnection GetRabbitMQConnection()
            {
                ConnectionFactory connectionFactory = new ConnectionFactory()
                {
                    // RabbitMQ'nun bağlantı kuracağı host'u tanımlıyoruz. Herhangi bir güvenlik önlemi koymak istersek, Management ekranından password adımlarını tanımlayıp factory içerisindeki "UserName" ve "Password" property'lerini set etmemiz yeterlidir.
                    HostName = _hostName
                };

                return connectionFactory.CreateConnection();
            }
        }
    }

    public class Publisher
    {
        private readonly Program.RabbitMQService _rabbitMQService;

        public Publisher(string queueName, string message)
        {
            _rabbitMQService = new Program.RabbitMQService();

            using (var connection = _rabbitMQService.GetRabbitMQConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.QueueDeclare(queueName, false, false, false, null);

                    channel.BasicPublish("", queueName, null, Encoding.UTF8.GetBytes(message));

                    Console.WriteLine("{0} queue'su üzerine, \"{1}\" mesajı yazıldı.", queueName, message);
                }
            }
        }
    }

    public class Consumer
    {
        private readonly Program.RabbitMQService _rabbitMQService;

        public Consumer(string queueName)
        {
            _rabbitMQService = new Program.RabbitMQService();

            using (var connection = _rabbitMQService.GetRabbitMQConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    var consumer = new EventingBasicConsumer(channel);
                    // Received event'i sürekli listen modunda olacaktır.
                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body.ToArray();
                        var message = Encoding.UTF8.GetString(body);

                        Console.WriteLine("{0} isimli queue üzerinden gelen mesaj: \"{1}\"", queueName, message);
                    };

                    channel.BasicConsume(queueName, true, consumer);
                }
            }
        }
    }
}
