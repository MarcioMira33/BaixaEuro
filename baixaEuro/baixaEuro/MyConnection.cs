using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace baixaEuro
{
    class MyConnection
    {
        public static SqlConnection conexao;
        public static SqlCommand cmd;
        public static SqlTransaction BeginTran;


        public static SqlConnection Conexao(string strcon) 
        {
            conexao = null;
            conexao = new SqlConnection(strcon);
            conexao.Open();
            return (conexao);
            
        }

        public static void fechaConexao(){

            conexao.Close();
        }

        public static SqlTransaction TransacaoBegin()
        {
            BeginTran = conexao.BeginTransaction();

            return (BeginTran);
        }

        public static SqlTransaction TransacaoCommit()
        {
            BeginTran.Commit();
            return (BeginTran);
        }

        public static SqlTransaction TransacaoRollback()
        {
            BeginTran.Rollback();
            return (BeginTran);
        }

        public static SqlCommand ComandoSQl(string Comando)
        {            
            cmd = new SqlCommand(Comando, conexao);   
            return (cmd);        
        }

        public SqlConnection conection (string strcon)
        {
            SqlConnection conexao = new SqlConnection(strcon);
            conexao.Open();
            return conexao;
        }

        public SqlDataReader buscaDados(string strcon, string sqlBusca)
        {
            conexao = conection(strcon);
            cmd = new SqlCommand(sqlBusca, conexao);
            cmd.ExecuteNonQuery(); // executa cmd
            SqlDataReader buscaParcela = cmd.ExecuteReader();
            return buscaParcela;
       
        }
    }
}
