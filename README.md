MaxDBProvider is ADO.NET data provider for .NET Standard/.NET Core and the SAP-certified database MaxDB 7.x, see [MaxDB](http://maxdb.sap.com).

Access MaxDB server using native network protocol.

Namespace MaxDB.Data and assembly MaxDB.Data.dll.

Connection String Format
`Server=localhost;Database=Test;User ID=MyLogin;Password=MyPwd;`

C# Example

```C#
using System;
using System.Data;
using MaxDB.Data;
 
public class Test
{
  public static void Main(string[] args)
  {
    string connectionString = "Server=localhost;Database=test;User ID=scott;Password=tiger;";
    using (var dbcon = new MaxDBConnection(connectionString))
    {
      dbcon.Open();
      using (var dbcmd = dbcon.CreateCommand())
      {
        // requires a table to be created named employee
        // with columns firstname and lastname such as,
        //        CREATE TABLE employee (
        //           firstname varchar(32),
        //           lastname varchar(32));
        string sql = SELECT firstname, lastname FROM employee";
        dbcmd.CommandText = sql;
        dbcmd.Connection = dbcon;
        using(var reader = dbcmd.ExecuteReader())
        {
          while (reader.Read())
          {
            string FirstName = (string)reader["firstname"];
            string LastName = (string)reader["lastname"];
            Console.WriteLine($"Name: {FirstName} {LastName}");
          }
        }
      }
    }
  }
}
```
