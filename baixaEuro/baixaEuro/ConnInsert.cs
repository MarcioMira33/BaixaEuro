
using System.Data.SqlClient;

namespace baixaEuro
{
    class ConnInsert
    {
        public static SqlConnection conexao = null;

        public SqlConnection abrirConexao(string strcon) 
        {
            if (ConnInsert.conexao == null)
            {
                ConnInsert.conexao = new SqlConnection(strcon);
                ConnInsert.conexao.Open();
            }
            return (ConnInsert.conexao);
            
        }

        public  void fecharConexao(){
            if (ConnInsert.conexao != null) {
                ConnInsert.conexao.Close();
            }
            ConnInsert.conexao = null;
        }

    }
}
