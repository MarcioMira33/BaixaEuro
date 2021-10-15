using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.ComponentModel;
using System.Threading;


namespace baixaEuro
{
    class MyCisa
    {
        public void GravarHeader(string LinhaHeader, string TotalLinhas, ref string conta, ref string ID_AX)
        {

            string Agencia;
            string ContaMovimento;
            string Cedente;
            string DataMovimento;
            string InserirSQL;
            //string IdRetorno;

            Agencia = LinhaHeader.Substring(26, 4);
            ContaMovimento = LinhaHeader.Substring(30, 8);
            Cedente = LinhaHeader.Substring(110, 7);
            DataMovimento = LinhaHeader.Substring(94, 6);
            conta = Agencia + ContaMovimento;

            DataMovimento = "20" + DataMovimento.Substring(4, 2) + "-" + DataMovimento.Substring(2, 2) + "-" + DataMovimento.Substring(0, 2);

            InserirSQL = " INSERT INTO PFAXHEADER (" +
                         " PFAXAGENCIA, PFAXCONTAMOV, PFAXCEDENTE, " +
                         " PFAXDATMOV, PFAXQTDTOTLINHA,  PFAXSTATUS ) " +
                         " OUTPUT INSERTED.PFAXID VALUES ( '" + Agencia + "', '" + ContaMovimento + "','" + Cedente + "','" +
                         DataMovimento + "'," + TotalLinhas + ",'L')";


            // string strcon = "Data Source=MEDUSA;Initial Catalog=BDSPFP;User ID=User_Bullish;Password=$#c&$$0(2O18)?*@%";
            // SqlConnection conexao = new SqlConnection(strcon);
            MyConnection.cmd = new SqlCommand(InserirSQL, MyConnection.conexao);

            try
            {
                //.Open(); // abre a conexão com o banco  
                // cmd.BeginExecuteNonQuery();
                MyConnection.cmd.Transaction = MyConnection.BeginTran;
                //MyConnection.cmd.ExecuteNonQuery(); // executa cmd
                //int idretorno = (int)MyConnection.cmd.ExecuteScalar();
                ID_AX = Convert.ToString(MyConnection.cmd.ExecuteScalar());
            }
            catch (Exception)
            {
                // EnviarEmail("Erro na Aplicação SentinelaCisa", "Error: Movimento Ceupi - data " + DataMovimento, "tatilon.Roberto@ceuma.br", "tatilon.roberto@ceuma.br");
                throw;
            }
            return;
        }
        public void GravarMovimento(string LinhaHeader, ref string ID_AX)
        {

            string seuNumero; // ---- 117-126
            string nossoNumero; //--- 127-134  
            string dataVencimento;//- 147-152
            string dataPagamento;//-- 296-301 
            string valorPago;//------ 254-266
            string valorTarifa;//---- 176-188
            string valorJurosAtraso;//202-214
            string valorIOFDevido;//--215-227
            string valorAbatimentoConcedido;//-- 228-240
            string valorDescontoConcedido;//---- 241-253
            string valorJurosMora;//------------ 267-279
            string nomeSacado; //--------------- 302-337
            string numeroLinha; //--------------- 395-400
            string numOcorrencia; //--------------- 395-400
            string numCarteira; //--------------- 395-400


            string InserirSQL;
            //string IdRetorno;

            // ID_AX   
            seuNumero = LinhaHeader.Substring(116, 10);
            nossoNumero = LinhaHeader.Substring(126, 8);
            dataVencimento = LinhaHeader.Substring(146, 6);
            dataPagamento = LinhaHeader.Substring(110, 6);
            valorPago = formatar(LinhaHeader.Substring(253, 13));
            valorDescontoConcedido = LinhaHeader.Substring(240, 13);
            valorTarifa = LinhaHeader.Substring(175, 13);
            valorJurosAtraso = LinhaHeader.Substring(201, 13);
            valorJurosMora = LinhaHeader.Substring(266, 13);
            valorJurosMora = String.Format("{0:c}", valorJurosMora);
            numOcorrencia = LinhaHeader.Substring(108, 2);
            numCarteira = LinhaHeader.Substring(107, 1);

            valorIOFDevido = LinhaHeader.Substring(214, 13);

            valorAbatimentoConcedido = LinhaHeader.Substring(227, 13);
            nomeSacado = LinhaHeader.Substring(301, 36);

            numeroLinha = LinhaHeader.Substring(394, 6);

            // convertendo data de arquivo para o padrao da base de dados AAAA-MM-DD
            dataVencimento = "20" + dataVencimento.Substring(4, 2) + "-" + dataVencimento.Substring(2, 2) + "-" + dataVencimento.Substring(0, 2);
            dataPagamento = "20" + dataPagamento.Substring(4, 2) + "-" + dataPagamento.Substring(2, 2) + "-" + dataPagamento.Substring(0, 2);

            // desenvolvendo aqui
            InserirSQL = " INSERT INTO PFAXMOVIMENTO (" +
                         " " + "FKAXID" + ", " + //                          
                         " " + "PFAXMOVSEUNUM" + ", " +
                          " " + "PFAXMOVNOSSNUM" + ", " +
                         " " + "PFAXMOVDATVEN" + ", " +
                         " " + "PFAXMOVDATPAG" + ", " +
                         " " + "PFAXMOVVALPAG" + ", " +
                         " " + "PFAXMOVVALDESCONTOCONCEDIDO" + ", " +
                         " " + "PFAXMOVTARIFA" + ", " +
                         " " + "PFAXMOVJUROSATRASO" + ", " +
                         " " + "PFAXMOVJUROSMORA" + ", " +
                         " " + "PFAXVALORIOFDEVIDO" + ", " +
                         " " + "PFAXNOMESACADO" + ", " +
                         " " + "PFAXNUMLINHA" + ", " +
                         " " + "PFAXOCORRENCIA" + ", " +
                         " " + "PFAXCARTEIRA" + ") " +
                         " " + " VALUES ( " + ID_AX + ", " +
                         " '" + seuNumero + "', " +
                          " " + nossoNumero + ", " +
                         " '" + dataVencimento + "', " +
                         " '" + dataPagamento + "', " +
                         " " + valorPago + ", " +
                         " " + valorDescontoConcedido + ", " +
                         " " + valorTarifa + ", " +
                         " " + valorJurosAtraso + ", " +
                           " " + valorJurosMora + ", " +
                         " " + valorIOFDevido + ", " +
                         " '" + nomeSacado + "', " +
                         " '" + numeroLinha + "', " +
                         " '" + numOcorrencia + "', " +
                         " " + numCarteira +
                         " " + ")";

            MyConnection.cmd = new SqlCommand(InserirSQL, MyConnection.conexao);
            try
            {
                //.Open(); // abre a conexão com o banco  
                // cmd.BeginExecuteNonQuery();
                MyConnection.cmd.Transaction = MyConnection.BeginTran;
                MyConnection.cmd.ExecuteNonQuery(); // executa cmd
                //int idretorno = (int)MyConnection.cmd.ExecuteScalar();
                //ID_AX = Convert.ToString(MyConnection.cmd.ExecuteScalar());    

            }
            catch (Exception)
            {
                // EnviarEmail("Erro na Aplicação SentinelaCisa", "Error: Movimento Ceupi - data " + DataMovimento, "tatilon.Roberto@ceuma.br", "tatilon.roberto@ceuma.br");
                MyConnection.cmd.Transaction.Rollback();

            }

            return;

        }

        public string formatar(string formatar)
        {
            string formatado;
            formatado = formatar.ToString().Substring(0, formatar.ToString().Length - 2) + "."
            + formatar.ToString().Substring(formatar.ToString().Length - 2);
            return formatado;
        }

        public void GravarTrailler(int quant, int valorTotal, string cnpj, ref string ID_AX)
        {
            string valor = "0";
            if (valorTotal != 0)
            {
                valor = formatar(valorTotal.ToString());
            }
            string InserirSQL;
            //string IdRetorno;
            // ID_AX   
            // desenvolvendo aqui
            InserirSQL = " INSERT INTO PFAXTRAILLER (" +
                         " " + "FKAXID" + ", " +
                         " " + "PFAXTRAQTDREGLIQ" + ", " +
                          " " + "PFAXTRAVALTOTLIQ" + ", " +
                         " " + "PFAXTRACNPJ" + ", " +
                         " " + "PFAXTRADATLEITURA )" + " " +
                         " " + " VALUES ( " + ID_AX + ", " +
                          " " + quant + ", " +
                         " '" + valor + "', " +
                         " '" + cnpj + "', " +
                         "  getDate()  )";

            MyConnection.cmd = new SqlCommand(InserirSQL, MyConnection.conexao);

            try
            {
                //.Open(); // abre a conexão com o banco  
                // cmd.BeginExecuteNonQuery();
                MyConnection.cmd.Transaction = MyConnection.BeginTran;
                MyConnection.cmd.ExecuteNonQuery(); // executa cmd
                //int idretorno = (int)MyConnection.cmd.ExecuteScalar();
                //ID_AX = Convert.ToString(MyConnection.cmd.ExecuteScalar());    

            }
            catch (Exception)
            {
                // EnviarEmail("Erro na Aplicação SentinelaCisa", "Error: Movimento Ceupi - data " + DataMovimento, "tatilon.Roberto@ceuma.br", "tatilon.roberto@ceuma.br");
                MyConnection.cmd.Transaction.Rollback();

            }

            return;

        }


        public static void EnviarEmail(string Assunto, string Mensagem, string Destino, string Origem)
        {
            // string Sistema = "SIS";
            string Sistema = "123456";
            string inseri = "";
            DateTime DataSistema = DateTime.Now;

            inseri = " INSERT INTO IN41MENSAT (IN41DESTIN,  FK41D0TIPDES, IN41TIPMID, IN41MIDIA, IN41MSG, IN41ENVIA, FK41D0TIPEVE, IN41DATAGE, IN41PROCES)" +
                     " VALUES ( '" + Sistema + "', '" + "G' ,'" + "E' , '" + Destino + "' , '" + Mensagem + "', '" + "N' , '" + "002' , '" + String.Format("{0:yyyy/MM/dd}", DataSistema) + " 00:00:00.000' , 'SISTEMA SENTINELA' )";


            string strcon = "Data Source=CL3DB;Initial Catalog=BDINTERP;User ID=User_GrpCeuma;Password=G*pC&#m@08.19?";
            SqlConnection conexao = new SqlConnection(strcon);
            SqlCommand cmd = new SqlCommand(inseri, conexao);
            try
            {
                conexao.Open(); // abre a conexão com o banco   
                cmd.ExecuteNonQuery(); // executa cmd
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                conexao.Close(); /* Se tudo ocorrer bem fecha a conexão com o banco da dados, sempre é bom fechar a conexão após executar até o final o que nos interessa, isso pode evitar problemas futuros */
            }

        }

        // ============================= CAMINHO PARA PASTA CISA DINAMICO
        public static string BuscarCaminho()
        { // pode ser incrementado com o IP ou nome do servido para referencia de unidade de IES
            string caminho = "";
            string buscar = "";

            buscar = " SELECT PFD0ITECOM + '#' + PFD0COMPLE + '#' + PFD0ITEDES AS CAMINHO " +
                     " FROM PFD0CONSTT WHERE PFD0TABELA  = 'CCISA'" +
                     " AND PFD0OBSERV = 'VALIDO'";

            string strcon = "Data Source=REIA;Initial Catalog=BDSPFP;User ID=User_Ceuma;Password=#nvC&#m@(2O19.4)?*";
            //Integrated Security=True; 

            SqlConnection conexao = new SqlConnection(strcon);
            SqlCommand cmd = new SqlCommand(buscar, conexao);

            try//Tenta executar o que estiver abaixo
            {
                conexao.Open(); // abre a conexão com o banco   
                cmd.ExecuteNonQuery(); // executa cmd

                SqlDataReader Leitor = cmd.ExecuteReader();

                if (Leitor.HasRows)
                {
                    while (Leitor.Read())
                    {
                        caminho = Leitor["CAMINHO"].ToString();
                    }
                }
                Leitor.Close();
                Leitor.Dispose();
            }
            catch (Exception ex)
            {
                //'MessageBox.Show("Erro " + ex.Message); /*Se ocorer algum erro será informado em um msgbox*/
                caminho = "Erro:" + "#" + ex.Message;
                return caminho;
                throw;
            }
            finally
            {
                conexao.Close(); /* Se tudo ocorrer bem fecha a conexão com o banco da dados, sempre é bom fechar a conexão após executar até o final o que nos interessa, isso pode evitar problemas futuros */
            }
            return caminho;
        }

        //------------------------------------------------------------------------------------------------------------------------------------

        public static bool ArquivoJaGravado(string strcon, string DataMov, string TotLinha)
        {
            string buscar = "";
            bool JaGravado = false;
            DataMov = "20" + DataMov.Substring(4, 2) + "-" + DataMov.Substring(2, 2) + "-" + DataMov.Substring(0, 2);

            buscar = " SELECT PFAXID   " +
                     " FROM PFAXHEADER WHERE " +
                     " PFAXDATMOV  = '" + DataMov + "'" +
                     " AND PFAXQTDTOTLINHA = " + TotLinha;

            //string strcon = "Data Source=REIA;Initial Catalog=BDSPFP;User ID=User_Ceuma;Password=#nvC&#m@(2O19.4)?*";
            //string strcon = "Data Source=MEDUSA;Initial Catalog=BDSPFP;User ID=User_Ceuma;Password=#nvC&#m@(2O19.4)?*";          

            SqlConnection conexao = new SqlConnection(strcon);
            SqlCommand cmd = new SqlCommand(buscar, conexao);

            try//Tenta executar o que estiver abaixo
            {
                conexao.Open(); // abre a conexão com o banco   
                cmd.ExecuteNonQuery(); // executa cmd

                SqlDataReader Leitor = cmd.ExecuteReader();

                if (Leitor.HasRows)
                {
                    while (Leitor.Read())
                    {
                        //caminho = Leitor["PFAXID"].ToString();
                        JaGravado = true;
                    }
                }
                Leitor.Close();
                Leitor.Dispose();
            }
            catch (Exception)
            {
                //'MessageBox.Show("Erro " + ex.Message); /*Se ocorer algum erro será informado em um msgbox*/
                //caminho = "Erro:" + "#" + ex.Message;
                JaGravado = false;
                throw;
            }

            finally
            {
                conexao.Close(); /* Se tudo ocorrer bem fecha a conexão com o banco da dados, sempre é bom fechar a conexão após executar até o final o que nos interessa, isso pode evitar problemas futuros */
            }
            return (JaGravado);
        }
    }
}
