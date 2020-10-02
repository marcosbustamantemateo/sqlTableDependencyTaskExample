using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading.Tasks;
using TableDependency.SqlClient;
using TableDependency.SqlClient.Base;
using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.EventArgs;

namespace SqlTableDependencyPrueba
{
    class Persona
    {
        public int Id { get; set; }
        public string Nombre { get; set; }
        public string Apellidos { get; set; }
        public int Edad { get; set; }
    }

    class PersonaDB
    {
        private static PersonaDB _instance = null;
        private static readonly object padlock = new object();
        private string connectionString = "Data Source= MBM; Initial Catalog= Prueba; user = administrador; password = B18822189b;";
        private string nombreTablaBD = "Persona";

        public static PersonaDB GetInstance()
        {
            if (_instance == null)
            {
                lock (padlock)
                {
                    if (_instance == null)
                    {
                        _instance = new PersonaDB();
                    }
                }
            }
            return _instance;
        }

        public List<Persona> Get()
        {
            List<Persona> personas = new List<Persona>();
            string sqlGaps = "SELECT * FROM {0}";
            string sql = String.Format(sqlGaps, nombreTablaBD);
            try
            {
                using (SqlConnection con = new SqlConnection(connectionString))
                {
                    con.Open();
                    using (SqlCommand command = new SqlCommand(sql, con))
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            Persona persona = new Persona
                            {
                                Id = Convert.ToInt32(reader.GetValue(0)),
                                Nombre = reader.GetValue(1).ToString().Trim(),
                                Apellidos = reader.GetValue(2).ToString().Trim(),
                                Edad = Convert.ToInt32(reader.GetValue(3))
                            };
                            personas.Add(persona);
                        }
                        reader.Close();
                        command.Dispose();
                    }
                    con.Close();
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR: " + e.Message);
            }
            return personas;
        }

        public void Listening()
        {           
            var mapper = new ModelToTableMapper<Persona>();
            mapper.AddMapping(c => c.Id, "Id");
            mapper.AddMapping(c => c.Nombre, "Nombre");
            mapper.AddMapping(c => c.Apellidos, "Apellidos");
            mapper.AddMapping(c => c.Edad, "Edad");
            using (var tableDependency = new SqlTableDependency<Persona>(connectionString, nombreTablaBD, mapper: mapper))
            {
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.OnError += TableDependency_Error;
                tableDependency.Start();

                Console.WriteLine();
                Console.WriteLine("Waiting for receiving notifications...");
                Console.WriteLine("Press a key to stop");
                Console.ReadKey();
                tableDependency.Stop();
            }
        }

        void TableDependency_Changed(object sender, RecordChangedEventArgs<Persona> e)
        {
            if (e.ChangeType != ChangeType.None)
            {
                var changedEntity = e.Entity;
                Console.WriteLine();
                Console.WriteLine("-----------------------------");
                Console.WriteLine("  DML operation: " + e.ChangeType);
                Console.WriteLine("-----------------------------");

                Console.WriteLine("ID: " + changedEntity.Id);
                Console.WriteLine("Nombre: " + changedEntity.Nombre);
                Console.WriteLine("Apellidos: " + changedEntity.Apellidos);
                Console.WriteLine("Edad: " + changedEntity.Edad);
            }
        }

        void TableDependency_Error(object sender, ErrorEventArgs e)
        {
            Console.WriteLine("ERROR: " + e.Error);                    
        }
    }

    class Util
    {
        public static void Print(List<Persona> personas)
        {
            Console.WriteLine(TabText("ID", 4) + TabText("NOMBRE", 8) + TabText("APELLIDOS", 12) + TabText("EDAD", 1));
            Console.WriteLine("-----------------------------");
            foreach (var persona in personas)
            {
                string mensaje = TabText(persona.Id.ToString(), 4) + TabText(persona.Nombre, 8)
                    + TabText(persona.Apellidos, 12) + TabText(persona.Edad.ToString(), 1);
                Console.WriteLine(mensaje);
            }
            Console.WriteLine("------------------------------");
        }

        public static string TabText(string value, int espacios)
        {
            value = value.Trim();
            if (value.Length < espacios)
            {
                for (int i = value.Length; i < espacios; i++)
                {
                    value += " ";
                }
            }
            return value;
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            Task task = Task.Factory.StartNew(() => PersonaDB.GetInstance().Listening());
            Task task2 = Task.Factory.StartNew(() => {
                List<Persona> personas = PersonaDB.GetInstance().Get();
                Util.Print(personas);
            });
            Task.WaitAll(task, task2);
                Console.WriteLine("All threads complete");
        }  
    }
}
