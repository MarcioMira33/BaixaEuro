using System;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.ServiceProcess;
using System.Threading;

namespace baixaEuro
{
    public partial class Service1 : ServiceBase
    {
        public Service1()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            EventLog.WriteEntry("Meu Serviço foi Inciado Finance Auto Baixa-FAMAZ", EventLogEntryType.Warning);
            ThreadStart start = new ThreadStart(AtivarBaixaService);
            Thread thread = new Thread(start);
            thread.Start();
        }

        protected override void OnStop()
        {
            EventLog.WriteEntry("Meu Serviço Parou! ", EventLogEntryType.Warning);

        }

        public void AtivarBaixaService()
        {
            string stringCon = "Data Source=VENEZA.UNIEURO.INT;Initial Catalog=BDSPFP;User ID=User_Ceuma;Password=#nvC&#m@(2O19.4)?*";
            int cont = 0;
            while (true)
            {
                cont++;
                Thread.Sleep(5000);
                if (servicoAtivo(stringCon))
                {
                    if (cont == 1)
                    {
                        EventLog.WriteEntry("Serviço está ativo na base - EURO ", EventLogEntryType.Warning);
                    }

                    if (horario(stringCon) || ativacaoIndependente(stringCon))
                    {
                        EventLog.WriteEntry("Serviço esta executando tarefas - EURO", EventLogEntryType.Warning); 
                        buscaPendenciasBaixa(stringCon);
                    }
                    else
                    {
                        EventLog.WriteEntry("Serviço não esta no horario - EURO", EventLogEntryType.Warning);
                    }

                    if (ativoInternetCaixa(stringCon))
                    {
                       EventLog.WriteEntry("Baixando Internet e Caixa - EURO", EventLogEntryType.Warning);
                       nfeInternetCaixa(stringCon);
                    }               
                }
            }
        }
        public bool horario(string sConexao)
        {
            bool retorno;
            string sql;

            sql = "SELECT REPLACE(PFD0ITECOM,':','') as INICIO, REPLACE(PFD0OBSERV,':','') AS FIM FROM PFD0CONSTT"
                + " WHERE PFD0TABELA = 'BAIXAH'";

            Connection con = new Connection();
            con.abrirConexao(sConexao);
            SqlDataReader rs = con.buscaDados(sql);

            if (rs.HasRows)
            {
                string horario = DateTime.Now.Hour.ToString() + DateTime.Now.Minute.ToString(); ;
                rs.Read();
                if (Int32.Parse(rs["INICIO"].ToString()) <= Int32.Parse(horario) &&
                    Int32.Parse(rs["FIM"].ToString()) >= Int32.Parse(horario))
                {
                    retorno = true;
                }
                else
                {
                    retorno = false;
                }
            }
            else
            {
                retorno = false;
            }
            con.fecharConexao();
            return retorno;
        }
        public bool servicoAtivo(string stringCon)
        {
            bool retorno;
            string sql = "SELECT * FROM PFD0CONSTT "
                + "  WHERE PFD0TABELA = 'BAIXAA'"
                + "  AND PFD0COMPLE = 'S'";
            Connection con = new Connection();
            con.abrirConexao(stringCon);
            SqlDataReader rs = con.buscaDados(sql);

            if (rs.HasRows)
            {
                retorno = true;
            }
            else
            {
                retorno = false;
            }
            con.fecharConexao();
            return retorno;
        }
        public bool ativacaoIndependente(string stringCon)
        {
            bool retorno;
            string sql = "SELECT * FROM PFD0CONSTT "
                + "  WHERE PFD0TABELA = 'BAIXAAI'"
                + "  AND PFD0COMPLE = 'S'";
            Connection con = new Connection();
            con.abrirConexao(stringCon);
            SqlDataReader rs = con.buscaDados(sql);

            if (rs.HasRows)
            {
                retorno = true;
            }
            else
            {
                retorno = false;
            }
            con.fecharConexao();
            return retorno;
        }
        public void buscaPendenciasBaixa(string stringCon)
        {
            string sLabel = "";
            string sql;
            string sSqlSource = "";
            string sConexao = "";
            string conta;

            sql = " SELECT HE.PFAXID AS ID, HE.PFAXAGENCIA+PFAXCONTAMOV AS CONTA FROM PFAXHEADER HE "
                  + " INNER JOIN PFAXTRAILLER TR ON TR.FKAXID = HE.PFAXID"
                  + " WHERE HE.PFAXCEDENTE <> 'PRONTO' "
                  + " AND HE.PFAXDATBAIXA IS NULL"
                  + " AND HE.PFAXUSUBAIXA IS NULL";

            Connection con = new Connection();
            con.abrirConexao(stringCon);
            DataTable tabelaIns = CarregaTabela(con.buscaDados(sql));
            con.fecharConexao();
            BaixaBoleto baixa;
            foreach (DataRow row in tabelaIns.Rows)
            {
                string id = row["ID"].ToString();
                conta = row["CONTA"].ToString();
                baixa = new BaixaBoleto();
                defIESLeitura(ref conta, ref sSqlSource, ref sLabel, ref sConexao);
                baixa.baixaAlt(sSqlSource, sConexao, conta, sLabel, ref id);
                registraBaixa(id, stringCon);
                baixa = null;
            }
        }
        public DataTable CarregaTabela(SqlDataReader reader)
        {
            DataTable tbRetorno = new DataTable();
            tbRetorno.Load(reader);
            return tbRetorno;
        }
        public void registraBaixa(string id, string sConexao)
        {
            string sql;
            sql = " UPDATE PFAXHEADER "
               + "       SET PFAXDATBAIXA = GETDATE() , "
               + "       PFAXUSUBAIXA = 'REAL'"
               + "  WHERE  PFAXID = " + id;

            ConnInsert classCon = new ConnInsert();
            SqlConnection con = classCon.abrirConexao(sConexao);
            SqlCommand cmd1 = con.CreateCommand();
            SqlTransaction tran = con.BeginTransaction();

            cmd1.CommandText = sql;
            cmd1.Transaction = tran;

            try
            {
                cmd1.ExecuteNonQuery(); // EXECUTA CMD
            }
            catch
            {
                tran.Rollback();
                con.Close();
                classCon.fecharConexao();
                classCon = null;
                tran = null;

            }
            finally
            {
                tran.Commit();
                con.Close();
                classCon.fecharConexao();
            }
        }
        public void defIESLeitura(ref string conta, ref string sSqlSource, ref string sLabel, ref string sConexao)
        {
            switch (conta)
            {
                case "384613000324":
                    sLabel = "UNIEURO";
                    sSqlSource = "VENEZA.UNIEURO.INT";
                    break;

                case "331313000263":
                    sLabel = "UNICEUMA";
                    sSqlSource = "CL3DB.CEUMA.EDU.BR";
                    break;

                case "331313002547":
                    sLabel = "IMPERATRIZ";
                    sSqlSource = "CL3DB.CEUMA.EDU.BR";
                    break;

                case "439413000755":
                    sLabel = "FAMAZ";
                    sSqlSource = "GENESIS.CEUMA.EDU.BR";
                    break;

                case "432613001674":
                    sLabel = "CEUPI";
                    sSqlSource = "REIA.CEUMA.EDU.BR";
                    break;

            }
            //string de conexão -- tirar user ceuma @@ 
            sConexao = "Data Source=" + sSqlSource + ";Initial Catalog=BDSPFP;User ID=User_GrpCeuma;Password=G*pC&#m@08.19?";
        }
        public void nfeInternetCaixa(string stringCon)
        {
            bool laco = true;
            bool graduacao = true;
            string sTab = "PF";
            string sql = "";
            DadosBoletoBaixa baixa = new DadosBoletoBaixa();
            baixa = new DadosBoletoBaixa();
            while (laco)
            {

                if (!graduacao)
                {
                    laco = false;
                    stringCon = "Data Source=VENEZA.UNIEURO.INT;Initial Catalog=BDEXTENSAOP;User ID=User_Ceuma;Password=#nvC&#m@(2O19.4)?*";
                    sTab = "EX";
                    baixa.bExtensao = true;
                    baixa.bGraduacao = false;

                }
                else
                {
                    baixa.bGraduacao = true;
                    baixa.bExtensao = false;
                }


                baixa.sStringConexao = stringCon;
                SqlConnection conexao;
                SqlCommand cmd;
                SqlDataReader buscaPag;

                //PAGAMENTOS DA INTERNET

                baixa.bNfeinternetcaixa = true;
                baixa.sStringConexao = stringCon;

                sql = "SELECT FK7637CPDALU, " + sTab + "76ANOREG AS PF76ANOREG, " + sTab + "76SEQREG AS PF76SEQREG, " + sTab + "76VALPAG AS PF76VALPAG, " + sTab + "76DATEVE AS  PF76DATEVE, " + sTab + "76CODCAM AS PF76CODCAM FROM " + sTab + "76MOVFIT P76 "
                     + " WHERE " + sTab + "76DATEVE BETWEEN CONVERT(datetime, CONVERT(varchar, GETDATE(), 102)) AND GETDATE()"
                     + " AND " + sTab + "76INDEST IS NULL AND " + sTab + "76DATEST IS NULL"
                     + " AND " + sTab + "76USUCAI IN('INTERNET')";

                Connection con = new Connection();
                con.abrirConexao(stringCon);
                DataTable tabelaIns = CarregaTabela(con.buscaDados(sql));
                con.fecharConexao();
                foreach (DataRow row in tabelaIns.Rows)
                {
                    baixa.dValorPago = decimal.Parse(row["PF76VALPAG"].ToString());
                    baixa.sDataPagamento = formataData(row["PF76DATEVE"].ToString());
                    baixa.sAnoReg = row["PF76ANOREG"].ToString();
                    baixa.sSeqReg = row["PF76SEQREG"].ToString();
                    baixa.sCpdAluno = row["FK7637CPDALU"].ToString();
                    baixa.sCodCam = row["PF76CODCAM"].ToString();
                    baixa.sNossoNumero = baixa.sSeqReg;



                    if (graduacao)
                    {
                        sql = " SELECT PF47SITBOL FROM PF47BOLETT"
                            + " WHERE  PF76ANOREG = " + baixa.sAnoReg
                            + " AND PF76SEQREG = " + baixa.sSeqReg;

                    }
                    else
                    {
                        sql = " SELECT EX47SITBOL AS  PF47SITBOL FROM EX47BOLETT"
                            + " WHERE FK4776ANOREG = " + baixa.sAnoReg
                            + " AND FK4776SEQREG = " + baixa.sSeqReg;
                    }

                    conexao = new SqlConnection(stringCon);
                    cmd = new SqlCommand(sql, conexao);
                    conexao.Open();
                    buscaPag = cmd.ExecuteReader();

                    if (buscaPag.HasRows)
                    {
                        while (buscaPag.Read())
                        {
                            if (buscaPag["PF47SITBOL"].ToString() == "1")
                            {
                                baixa.bMensalidade = true;
                                baixa.bNegociacao = false;
                                baixa.sCodProdutoNfe = "0001";
                            }
                            else if (buscaPag["PF47SITBOL"].ToString() == "4")
                            {
                                baixa.bMensalidade = false;
                                baixa.bNegociacao = true;
                                baixa.sCodProdutoNfe = "0002";
                            }
                        }

                        EnvioNfe nfe = new EnvioNfe();
                        nfe.gravaDadosNota(ref baixa);
                    }
                }




                //PAGAMENTOS DO CAIXA-----------------------------------------------------------------------------------------------------

                baixa.bNfeinternetcaixa = true;
                baixa.sStringConexao = stringCon;

                sql = "SELECT FK7637CPDALU, " + sTab + "76ANOREG AS PF76ANOREG, " + sTab + "76SEQREG AS PF76SEQREG, " + sTab + "76VALPAG AS PF76VALPAG, " + sTab + "76DATEVE AS  PF76DATEVE, " + sTab + "76CODCAM AS PF76CODCAM FROM " + sTab + "76MOVFIT P76 "
                     + " WHERE " + sTab + "76DATEVE BETWEEN CONVERT(datetime, CONVERT(varchar, GETDATE(), 102)) AND GETDATE()"
                     + " AND " + sTab + "76INDEST IS NULL AND " + sTab + "76DATEST IS NULL"
                     + " AND " + sTab + "76USUCAI NOT IN('REAL','PRAVALER', 'INTERNET','Acordo')";

                con = new Connection();
                con.abrirConexao(stringCon);
                tabelaIns = CarregaTabela(con.buscaDados(sql));
                con.fecharConexao();
                foreach (DataRow row in tabelaIns.Rows)
                {
                    baixa.dValorPago = decimal.Parse(row["PF76VALPAG"].ToString());
                    baixa.sDataPagamento = formataData(row["PF76DATEVE"].ToString());
                    baixa.sAnoReg = row["PF76ANOREG"].ToString();
                    baixa.sSeqReg = row["PF76SEQREG"].ToString();
                    baixa.sCpdAluno = row["FK7637CPDALU"].ToString();
                    baixa.sCodCam = row["PF76CODCAM"].ToString();
                    baixa.sNossoNumero = baixa.sSeqReg;


                    if (graduacao)
                    {
                        sql = " SELECT PF47SITBOL FROM PF47BOLETT"
                            + " WHERE  PF76ANOREG = " + baixa.sAnoReg
                            + " AND PF76SEQREG = " + baixa.sSeqReg;

                    }
                    else
                    {
                        sql = " SELECT EX47SITBOL AS  PF47SITBOL FROM EX47BOLETT"
                            + " WHERE FK4776ANOREG = " + baixa.sAnoReg
                            + " AND FK4776SEQREG = " + baixa.sSeqReg;
                    }




                    conexao = new SqlConnection(stringCon);
                    cmd = new SqlCommand(sql, conexao);
                    conexao.Open();
                    buscaPag = cmd.ExecuteReader();

                    if (buscaPag.HasRows)
                    {
                        while (buscaPag.Read())
                        {
                            if (buscaPag["PF47SITBOL"].ToString() == "1")
                            {
                                baixa.bMensalidade = true;
                                baixa.bNegociacao = false;
                                baixa.sCodProdutoNfe = "0001";
                            }
                            else if (buscaPag["PF47SITBOL"].ToString() == "4")
                            {
                                baixa.bMensalidade = false;
                                baixa.bNegociacao = true;
                                baixa.sCodProdutoNfe = "0002";
                            }
                        }

                        EnvioNfe nfe = new EnvioNfe();
                        nfe.gravaDadosNota(ref baixa);
                    }

                }
                if (!graduacao)
                {
                    laco = false;
                }
                graduacao = false;
            }
        }
        public string formataData(string data)
        {
            DateTime oDate = Convert.ToDateTime(data);
            data = oDate.Year + "-" + oDate.Month + "-" + oDate.Day;
            return data;
        }
        public bool ativoInternetCaixa(string stringCon)
        {
            bool retorno;
            string sql = "SELECT * FROM PFD0CONSTT "
                + "  WHERE PFD0TABELA = 'BAIXAIC'"
                + "  AND PFD0COMPLE = 'S'";
            Connection con = new Connection();
            con.abrirConexao(stringCon);
            SqlDataReader rs = con.buscaDados(sql);

            if (rs.HasRows)
            {
                retorno = true;
            }
            else
            {
                retorno = false;
            }
            con.fecharConexao();
            return retorno;
        }
    }
}
