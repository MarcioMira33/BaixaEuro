
using System.Data.SqlClient;

namespace baixaEuro
{
    class Connection
    {
        public static SqlConnection conexao = null;
        public SqlCommand cmd;

        public SqlConnection abrirConexao(string strcon) 
        {
            if (Connection.conexao == null)
            {
                Connection.conexao = new SqlConnection(strcon);
                Connection.conexao.Open();
            }
            return (Connection.conexao);
            
        }

        public  void fecharConexao(){
            if (Connection.conexao != null) {
                Connection.conexao.Close();
            }
            Connection.conexao = null;
        }

        public SqlCommand ComandoSQl(string Comando)
        {
            this.cmd = new SqlCommand(Comando, Connection.conexao);   
            return (this.cmd);        
        }

        public SqlDataReader buscaDados(string sqlBusca)
        {
            this.cmd = ComandoSQl(sqlBusca);
            this.cmd.ExecuteNonQuery(); // executa cmd
            SqlDataReader busca = this.cmd.ExecuteReader();
            return busca;
        }
    }
}
