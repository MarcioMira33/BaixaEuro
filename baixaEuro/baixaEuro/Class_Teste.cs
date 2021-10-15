using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Threading;

namespace baixaEuro
{
    public class Class_Teste
    {
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
                        //EventLog.WriteEntry("Serviço está ativo na base - EURO ", EventLogEntryType.Warning);
                    }

                    if (horario(stringCon) || ativacaoIndependente(stringCon))
                    {
                        //EventLog.WriteEntry("Serviço esta executando tarefas - EURO", EventLogEntryType.Warning);
                        buscaPendenciasBaixa(stringCon);
                    }
                    else
                    {
                       // EventLog.WriteEntry("Serviço não esta no horario - EURO", EventLogEntryType.Warning);
                    }

                    if (ativoInternetCaixa(stringCon))
                    {
                        //EventLog.WriteEntry("Baixando Internet e Caixa - EURO", EventLogEntryType.Warning);
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
        public bool ativaBaixaPendencia(ref bool ativoPendencias, string stringCon)
        {
            bool retorno;
            string sql = "SELECT PFD0COMPLE AS ATIVAR FROM PFD0CONSTT"
                    + " WHERE PFD0TABELA = 'BAIXAP'";

            Connection con = new Connection();
            con.abrirConexao(stringCon);
            SqlDataReader rs = con.buscaDados(sql);

            if (rs.HasRows)
            {
                rs.Read();
                if (rs["ATIVAR"].ToString() == "S")
                {
                    ativoPendencias = true;
                    retorno = true;
                }
                else
                {
                    ativoPendencias = false;
                    retorno = false;
                }
            }
            else
            {
                ativoPendencias = false;
                retorno = false;
            }
            con.fecharConexao();
            return retorno;
        }
        public string[] BuscarCaminho()
        {
            string aux = "";
            string buscar ;
            string sConexao ;
            string[] caminho;

            buscar = " SELECT PFD0ITECOM + '#' + PFD0COMPLE + '#' + PFD0ITEDES AS CAMINHO " +
                     " FROM PFD0CONSTT WHERE PFD0TABELA  = 'CCISA'" +
                     " AND PFD0OBSERV = 'VALIDO'";
            sConexao = "Data Source=CL3DB;Initial Catalog=BDSPFP;User ID=User_GrpCeuma;Password=G*pC&#m@08.19?";
            SqlConnection conexao = new SqlConnection(sConexao);
            conexao.Open();
            SqlCommand cmd = new SqlCommand(buscar, conexao);


            try //Tenta executar o que estiver abaixo
            {

                cmd.ExecuteNonQuery();
                SqlDataReader leitor = cmd.ExecuteReader();

                if (leitor.HasRows)
                {
                    while (leitor.Read())
                    {
                        aux = leitor["CAMINHO"].ToString();
                    }
                }
                leitor.Close();
                leitor.Dispose();
            }
            catch (Exception ex)
            {
                aux = "Erro:" + "#" + ex.Message;
                throw;
            }
            finally
            {
                conexao.Close();
                caminho = aux.Substring(0).Split('#');
            }

            return caminho;
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
        public void verificaProuniFies(string stringCon, string iess)
        {
            string sql;

            sql = " SELECT p47.FK4737CPDALU as CPD FROM PF47BOLETT P47" +
                  " inner join PFB9BOLSAT bol on bol.PFB9CPDALU = p47.FK4737CPDALU" +
                  " and bol.PFB9ANOREF = p47.PF47ANOBOL" +
                  " WHERE P47.PF47ANOBOL = 20202" +
                  " and P47.PF47CODBOL = 01" +
                  " and P47.PF47SITBOL in ('1', '4', '6')" +
                  " AND bol.PFB9STATUS = 'A'";

            if (iess == "CEUMA")
            {
                sql = sql + " and bol.FKB9B8CODMOT in ('23', '11')";
            }
            else if (iess == "EURO")
            {
                sql = sql + " and bol.FKB9B8CODMOT in ('23', 'I9')";
            }

            Connection cConn = new Connection();
            cConn.abrirConexao(stringCon);
            SqlDataReader retornoVal = cConn.buscaDados(sql);

            if (retornoVal.HasRows)
            {
                while (retornoVal.Read())
                {

                    string sqlBolsaUp;
                    sqlBolsaUp = " UPDATE PFB9BOLSAT" +
                                 " SET PFB9STATUS = 'C' " +
                                 " WHERE 1 = 1 " +
                                 " AND PFB9CPDALU = " +
                                 " AND PFB9ANOREF = 20202 " +
                                 " AND PFB9STATUS = 'A' ";

                    if (iess == "CEUMA")
                    {
                        sqlBolsaUp = sqlBolsaUp + " and bol.FKB9B8CODMOT in ('23', '11')";
                    }
                    else if (iess == "EURO")
                    {
                        sqlBolsaUp = sqlBolsaUp + " and bol.FKB9B8CODMOT in ('23', 'I9')";
                    }

                    ConnInsert classCon = new ConnInsert();
                    SqlConnection con = classCon.abrirConexao(stringCon);
                    SqlTransaction tranBolsa = con.BeginTransaction();

                    SqlCommand cmd1 = con.CreateCommand();
                    try
                    {
                        cmd1.CommandText = sqlBolsaUp;
                        cmd1.Transaction = tranBolsa;
                        cmd1.ExecuteNonQuery(); // EXECUTA CMD
                    }
                    catch (Exception)
                    {
                        tranBolsa.Rollback();
                        classCon.fecharConexao();

                    }
                    finally
                    {
                        tranBolsa.Commit();
                        classCon.fecharConexao();

                    }
                }
                cConn.fecharConexao();
            }
            else
            {
                cConn.fecharConexao();
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
        public void baixarBoletos()
        {

            string strcon = "Data Source=CL3DB.CEUMA.EDU.BR;Initial Catalog=BDSPFP;User ID=User_GrpCeuma;Password=G*pC&#m@08.19?";
            string sqlSource = "CL3DB.CEUMA.EDU.BR";
            string conta = "331313000263";
            string sLabel = "UNICEUMA";

            string sql76 = "";
            sql76 = " SELECT * FROM PFAXMOVIMENTO PAX "
              + " WHERE YEAR(PFAXMOVDATPAG) = 2020 "
              + " AND MONTH(PFAXMOVDATPAG)= 6"
              + " AND PFAXOCORRENCIA = 6 "
              + " AND LEFT(PFAXMOVNOSSNUM,1) = 2";


            SqlConnection conexao = new SqlConnection(strcon);
            SqlCommand cmd = new SqlCommand(sql76, conexao);
            conexao.Open();
            SqlDataReader buscaAxMov = cmd.ExecuteReader();
            if (buscaAxMov.HasRows)
            {
                DadosBoletoBaixa baixa;
                while (buscaAxMov.Read())
                {
                    string tipobol = buscaAxMov["PFAXMOVSEUNUM"].ToString().Substring(0, 1);
                    // tipobol = 0 / Graduacao
                    // tipobol = 1 / Extensão
                    // tipobol = 2

                    baixa = null;
                    baixa = new DadosBoletoBaixa();
                    baixa.sStringConexao = strcon;
                    baixa.sSqlSource = sqlSource;
                    baixa.carregaClasse(buscaAxMov);
                    baixa.sAgeCon = conta;
                    baixa.identIes(sLabel);

                    // if (baixa.sTipoBol == "1")
                    //{
                    if (baixa.bAchouX9 && baixa.bExtensao && !baixa.bVestibular)
                    {
                        baixa.sStringConexao = "Data Source=CL3DB.CEUMA.EDU.BR;Initial Catalog=BDEXTENSAOP;User ID=User_GrpCeuma;Password=G*pC&#m@08.19?";
                        string sql = " SELECT * FROM EX47BOLETT WHERE FK4776ANOREG = " + baixa.sAnoReg + " AND FK4776SEQREG = " + baixa.sSeqReg;
                        SqlConnection conn = new SqlConnection(baixa.sStringConexao);
                        conn.Open();
                        SqlCommand cmdd = new SqlCommand(sql, conn);
                        cmdd.ExecuteNonQuery(); // executa cmd
                        SqlDataReader buscapag = cmdd.ExecuteReader();
                        if (!buscapag.HasRows)
                        {
                            baixa.bRollback = false;
                            conn.Close();
                            ConnInsert classCon = new ConnInsert();
                            SqlConnection con = classCon.abrirConexao(baixa.sStringConexao);
                            SqlTransaction tranBaixa = con.BeginTransaction();

                            if (baixa.bExtensao && !baixa.bPreInscricao)
                            {
                                InsereExtensao insereExtensao = new InsereExtensao();
                                insereExtensao.inserir(ref baixa, ref tranBaixa, ref con);
                            }
                            if (baixa.bRollback)
                            {
                                tranBaixa.Rollback();
                                con.Close();
                                classCon.fecharConexao();
                                classCon = null;
                            }
                            else
                            {
                                tranBaixa.Commit();
                                con.Close();
                                classCon.fecharConexao();
                                classCon = null;
                                tranBaixa = null;
                            }
                        }
                        //}
                    }
                }
            }
        }
        public void buscaNaoBaixados(string stringCon, string data)
        {
            string sLabel = "";
            string sql;
            string sSqlSource = "";
            string sConexao = "";
            string conta;

            sql = " SELECT PFAXID AS ID, PFAXAGENCIA+PFAXCONTAMOV AS CONTA "
                + " FROM PFAXHEADER"
                + " inner join PFAXTRAILLER ON FKAXID = PFAXID"
                + " AND PFAXCEDENTE <> 'PRONTO'"
                + " and  PFAXDATMOV = '" + data + "'";

            Connection con = new Connection();
            con.abrirConexao(stringCon);
            DataTable tabelaIns = CarregaTabela(con.buscaDados(sql));
            con.fecharConexao();
            foreach (DataRow row in tabelaIns.Rows)
            {
                string id = row["ID"].ToString();
                conta = row["CONTA"].ToString();
                defIESLeitura(ref conta, ref sSqlSource, ref sLabel, ref sConexao);
                baixaAlt(sSqlSource, sConexao, conta, sLabel, ref id);
                registraBaixa(id, stringCon);
            }
        }
        public void baixaAlt(string sqlSource, string strcon, string conta, string sLabel, ref string ID_AX)
        {
            string nnnumero = "";
            string buscarAX;

            buscarAX = "";
            buscarAX += " SELECT * FROM PFAXMOVIMENTO ";
            buscarAX += " WHERE 1=1 ";
            buscarAX += "       AND PFAXOCORRENCIA IN (6) ";
            buscarAX += "       AND FKAXID = " + ID_AX;
            //buscarAX += " and PFAXMOVNOSSNUM in (10680446)";

            SqlConnection conexao = new SqlConnection(strcon);
            SqlCommand cmd;

            try
            {
                conexao.Open();
                cmd = new SqlCommand(buscarAX, conexao);
                cmd.ExecuteNonQuery(); // executa cmd
                SqlDataReader buscaAxMov = cmd.ExecuteReader();
                if (buscaAxMov.HasRows)
                {
                    DadosBoletoBaixa baixa;
                    EnvioNfe notaFiscal;
                    while (buscaAxMov.Read())
                    {
                        // tipobol = 0 / Graduacao
                        // tipobol = 1 / Extensão
                        // tipobol = 2

                        baixa = null;
                        baixa = new DadosBoletoBaixa();
                        baixa.sStringConexao = strcon;
                        baixa.sSqlSource = sqlSource;
                        baixa.carregaClasse(buscaAxMov);
                        baixa.sAgeCon = conta;
                        baixa.identIes(sLabel);
                        nnnumero = baixa.sNossoNumero;


                        if (baixa.bAchouX9 && !baixa.bPreInscricao)
                        {
                            if (baixa.sTipoOcorrencia == "6" && !baixa.bVestibular)
                            {

                                if (baixa.sSeqReg != null && baixa.sAnoReg != null)
                                {
                                    baixa.bPagDuplicado = true;
                                    if (baixa.bEuro)
                                    {
                                      // notaFiscal = null;
                                      // notaFiscal = new EnvioNfe();
                                      // notaFiscal.gravaDadosNota(ref baixa);
                                    }
                                }
                                else
                                {

                                    baixa.preparaSqlInsert76();
                                    ConnInsert classCon = new ConnInsert();
                                    SqlConnection con = classCon.abrirConexao(baixa.sStringConexao);

                                    SqlCommand cmd1 = con.CreateCommand();
                                    SqlTransaction tran76 = con.BeginTransaction();
                                    try
                                    {
                                        cmd1.CommandText = baixa.sSqlInsere76;
                                        cmd1.Transaction = tran76;
                                        cmd1.ExecuteNonQuery();
                                        baixa.buscaW6(ref con, ref tran76);
                                        baixa.binseriu76 = true;
                                        tran76.Commit();
                                    }
                                    catch (Exception ex)
                                    {
                                        //SE TIVER ALGUM ERRO, ENTAO...
                                        tran76.Rollback();
                                        Console.WriteLine("Erro ao Baixar Arquivo PF76 : " + ID_AX + "Nosso Numero: " + baixa.sNossoNumero + " Erro: " + ex);
                                    }
                                    finally
                                    {

                                        con.Close();
                                        classCon.fecharConexao();
                                        classCon = null;
                                        tran76 = null;
                                        baixa.busca76();
                                        if (baixa.bEuro)
                                        {
                                          //notaFiscal = null;
                                         // notaFiscal = new EnvioNfe();
                                          //notaFiscal.gravaDadosNota(ref baixa);
                                        }
                                    }
                                }
                                if (baixa.binseriu76 && !baixa.bPagDuplicado)
                                {
                                    if (baixa.bBiblioteca)//não precisa
                                    {
                                        //ConnInsert classCon = new ConnInsert();
                                        //string conBiblioteca = "Data Source=CL2DB;Initial Catalog=BDSCBP;User ID=User_Ceuma;Password=#nvC&#m@(2O19.4)?*";
                                        //SqlConnection con = classCon.abrirConexao(conBiblioteca);
                                        //SqlTransaction tranB = con.BeginTransaction();

                                        //string sSql = "";
                                        //sSql = sSql + " UPDATE USU_USUARIOS SET USU_SALDO = (USU_SALDO + " + baixa.dValorPago + " ) " +
                                        //              " WHERE USU_CODIGO_NOVO = '" + baixa.sCpdAluno + "'";

                                        //SqlCommand cmdb = con.CreateCommand();
                                        //try
                                        //{
                                        //    cmdb.CommandText = sSql;
                                        //    cmdb.Transaction = tranB;
                                        //    cmdb.ExecuteNonQuery(); // EXECUTA CMD
                                        //    tranB.Commit();
                                        //    con.Close();
                                        //    classCon.fecharConexao();
                                        //    classCon = null;
                                        //    tranB = null;
                                        //}
                                        //catch
                                        //{
                                        //    tranB.Rollback();
                                        //    con.Close();
                                        //    classCon.fecharConexao();
                                        //    tranB = null;
                                        //    classCon = null;
                                        //    Console.WriteLine("Erro ao Baixar Pagamento de Biblioteca ID: " + ID_AX + " Nosso Numero: " + baixa.sNossoNumero);

                                        //}
                                    }

                                    else
                                    {
                                        ConnInsert classCon = new ConnInsert();
                                        SqlConnection con = classCon.abrirConexao(baixa.sStringConexao);
                                        SqlTransaction tranBaixa = con.BeginTransaction();

                                        //INICIA BAIXA DE BOLETOS
                                        if (baixa.bGraduacao)
                                        {
                                            InsereGraduacao insereGraduacao = new InsereGraduacao();
                                            insereGraduacao.inserir(ref baixa, ref tranBaixa, ref con);
                                        }
                                        else if (baixa.bExtensao && !baixa.bPreInscricao)
                                        {
                                            InsereExtensao insereExtensao = new InsereExtensao();
                                            insereExtensao.inserir(ref baixa, ref tranBaixa, ref con);
                                        }

                                        else if (baixa.bVestibular) //baixando ainda no legado
                                        {
                                         baixa.busca82(baixa.sServico_vest, ref con, ref tranBaixa);
                                         baixa.baixaVestibular(ref con, ref tranBaixa);
                                        }

                                        else if (baixa.bAchouVestibular)
                                        {
                                         baixa.alocarAluno(ref con, ref tranBaixa);
                                        }

                  

                                        //SE TIVER ALGUM ERRO, ENTAO...
                                        if (baixa.bRollback)
                                        {
                                            tranBaixa.Rollback();
                                            con.Close();
                                            classCon.fecharConexao();
                                            classCon = null;
                                            Console.WriteLine("Erro ao Quitar Parcela no Sistema: " + ID_AX + " Nosso Numero: " + baixa.sNossoNumero);
                                        }
                                        else
                                        {
                                            tranBaixa.Commit();
                                            con.Close();
                                            classCon.fecharConexao();
                                            classCon = null;
                                            tranBaixa = null;
                                        }
                                    }
                                }
                                else
                                {
                                    baixa.bPagDuplicado = true;
                                }
                            }
                            else if (baixa.sTipoOcorrencia == "2" && !baixa.bVestibular)
                            {
                                ConnInsert classCon = new ConnInsert();
                                SqlConnection con = classCon.abrirConexao(baixa.sStringConexao);
                                SqlTransaction tranBaixa = con.BeginTransaction();

                                baixa.buscaW6Ref2(ref con, ref tranBaixa);

                                if (baixa.bRollback)
                                {
                                    tranBaixa.Rollback();
                                    con.Close();
                                    classCon.fecharConexao();
                                    tranBaixa = null;
                                    classCon = null;
                                    Console.WriteLine("Erro ao Registrar Boleto na W9 no Sistema: " + ID_AX + " Nosso Numero: " + baixa.sNossoNumero);
                                }
                                else
                                {
                                    tranBaixa.Commit();
                                    con.Close();
                                    classCon.fecharConexao();
                                    classCon = null;
                                    tranBaixa = null;
                                }
                            }
                        }
                        else if (true)
                          //  (baixa.bExtensao && baixa.bPreInscricao)// NÃO ENTRAR NESSE METODO AGORA...//baixando ainda no legado
                        {

                           // if (baixa.sTipoOcorrencia == "6")
                           // {
                             //   baixa.identTipoPreInscricao();
                             //   baixa.baixaPreInscricao();
                           // }
                            //else
                            //{
                            //    ConnInsert classCon = new ConnInsert();
                            //    SqlConnection con = classCon.abrirConexao(baixa.sStringConexao);
                            //    SqlTransaction tranBaixa = con.BeginTransaction();

                            //    baixa.buscaW6Ref2(ref con, ref tranBaixa);

                            //    if (baixa.bRollback)
                            //    {
                            //        tranBaixa.Rollback();
                            //        con.Close();
                            //        classCon.fecharConexao();
                            //        classCon = null;
                            //        Console.WriteLine("Erro ao Registrar Boleto na W9 no Sistema: " + ID_AX + " Nosso Numero: " + baixa.sNossoNumero);
                            //    }
                            //    else
                            //    {
                            //        tranBaixa.Commit();
                            //        con.Close();
                            //        classCon.fecharConexao();
                            //        classCon = null;
                            //        tranBaixa = null;
                            //    }
                            //}
                        }//DESCOMENTAR DEPOIS DE TODOS OS TESTES...
                        else
                        {
                            Console.WriteLine("Baixa não encontrada na X9 com esse ID : " + ID_AX + "Nosso Numero: " + baixa.sNossoNumero);
                        }
                        if (baixa.bCartaCredito && !baixa.bPagDuplicado)
                        {
                            ConnInsert classCon = new ConnInsert();
                            SqlConnection con = classCon.abrirConexao(baixa.sStringConexao);
                            SqlTransaction tranCarta = con.BeginTransaction();

                            baixa.inserirCartaCredito(ref tranCarta, ref con);

                            if (baixa.bRollback)
                            {
                                tranCarta.Rollback();
                                con.Close();
                                classCon.fecharConexao();
                                classCon = null;
                                Console.WriteLine("Erro ao Inserir Carta de Credito no Sistema: " + ID_AX + " Nosso Numero: " + baixa.sNossoNumero);
                            }
                            else
                            {
                                tranCarta.Commit();
                                con.Close();
                                classCon.fecharConexao();
                                classCon = null;
                                tranCarta = null;
                            }
                        }
                    }//FIM do LAÇO
                }
                else
                {
                    Console.WriteLine("Não foram encontrados Baixas com esse ID : " + ID_AX);
                }

            }
            catch
            {
             }
            finally
            {
                //registraBaixa(ID_AX, strcon);
                Console.WriteLine("Baixa Finalizada ID : " + ID_AX + " || Data: " + DateTime.Now);
                conexao.Close();
                conexao = null;
            }
        }

        public void nfeInternetCaixa(string stringCon)
        {
            bool laco = true;
            bool graduacao = true;
            string sTab = "PF";
            string sql = "";
            DadosBoletoBaixa baixa = new DadosBoletoBaixa();
            baixa = new DadosBoletoBaixa();
            while (laco) {

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
                 + " AND " + sTab + "76USUCAI NOT IN('REAL','PRAVALER', 'INTERNET')";

                //Só para testar, depois comentar;
            sql = "SELECT FK7637CPDALU, " + sTab + "76ANOREG AS PF76ANOREG, " + sTab + "76SEQREG AS PF76SEQREG, " + sTab + "76VALPAG AS PF76VALPAG, " + sTab + "76DATEVE AS  PF76DATEVE, " + sTab + "76CODCAM AS PF76CODCAM FROM " + sTab + "76MOVFIT P76 "
                 + " WHERE " + sTab + "76DATEVE BETWEEN '2021-04-01 00:00:00' AND '2021-04-26 23:59:00'"
                 + " AND " + sTab + "76INDEST IS NULL AND " + sTab + "76DATEST IS NULL"
                 + " AND " + sTab + "76USUCAI ='INTERNET'";

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
                     + " AND " + sTab + "76USUCAI NOT IN('REAL','PRAVALER', 'INTERNET')";

                //Só para testar, depois comentar;
                sql = "SELECT FK7637CPDALU, " + sTab + "76ANOREG AS PF76ANOREG, " + sTab + "76SEQREG AS PF76SEQREG, " + sTab + "76VALPAG AS PF76VALPAG, " + sTab + "76DATEVE AS  PF76DATEVE, " + sTab + "76CODCAM AS PF76CODCAM FROM " + sTab + "76MOVFIT P76 "
                     + " WHERE " + sTab + "76DATEVE BETWEEN '2021-04-01 00:00:00' AND '2021-04-26 23:59:00'"
                     + " AND " + sTab + "76INDEST IS NULL AND " + sTab + "76DATEST IS NULL"
                     + " AND " + sTab + "76USUCAI NOT IN('REAL','PRAVALER', 'INTERNET')";

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
                if(graduacao){
                    laco = false;
                }
                graduacao = false;
            }
        }

        public void nfeEstorno()
        {

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

        public void baixarBoletos2()
        {

            string strcon;
            string sqlSource;
            string conta;
            string sLabel;

            strcon = "Data Source=VENEZA.UNIEURO.INT;Initial Catalog=BDSPFP;User ID=User_GrpCeuma;Password=G*pC&#m@08.19?";
            sqlSource = "VENEZA.UNIEURO.INT";          
            conta = "432613001674";
            sLabel = "UNIEURO";

            string sql76 = "";
            sql76 = " SELECT * FROM PFAXMOVIMENTO "
                    + " WHERE PFAXMOVNOSSNUM IN( "
                    + " SELECT P6.FK76X9BANCONOSSONUM FROM PF76MOVFIT P6 "
                    + " INNER JOIN PFX9NOSSONUM X9 ON X9.PFX9BANCONOSSONUM = P6.FK76X9BANCONOSSONUM "
                    + " AND X9.PFX9ANO = P6.PF76ANOREG "
                    + " AND X9.PFX9TIPOBOLETO = 2 "
                    + " WHERE P6.PF76ANOREG in (2020,2021) "
                    + " AND P6.PF76ANOBOL = 20211 "
                    + " AND P6.PF76SEQREG NOT IN "
                    + " ( "
                    + " SELECT P7.PF76SEQREG FROM PF47BOLETT P7 "
                    + " WHERE P7.PF76ANOREG = P6.PF76ANOREG "
                    + " AND P7.PF76SEQREG = P6.PF76SEQREG "
                    + " ) "
                    + " ) "
                    + " AND PFAXOCORRENCIA = 6 ";


            SqlConnection conexao = new SqlConnection(strcon);
            SqlCommand cmd = new SqlCommand(sql76, conexao);
            conexao.Open();
            SqlDataReader buscaAxMov = cmd.ExecuteReader();
            if (buscaAxMov.HasRows)
            {
                DadosBoletoBaixa baixa;
                while (buscaAxMov.Read())
                {
                    string tipobol = buscaAxMov["PFAXMOVSEUNUM"].ToString().Substring(0, 1);
                    // tipobol = 0 / Graduacao
                    // tipobol = 1 / Extensão
                    // tipobol = 2

                    baixa = null;
                    baixa = new DadosBoletoBaixa();
                    baixa.sStringConexao = strcon;
                    baixa.sSqlSource = sqlSource;
                    baixa.carregaClasse(buscaAxMov);
                    baixa.sAgeCon = conta;
                    baixa.identIes(sLabel);

                    // if (baixa.sTipoBol == "1")
                    //{
                    if (baixa.bAchouX9 && baixa.bGraduacao && !baixa.bVestibular)
                    {
                        //baixa.sStringConexao = "Data Source=CL3DB.CEUMA.EDU.BR;Initial Catalog=BDEXTENSAOP;User ID=User_GrpCeuma;Password=G*pC&#m@08.19?";
                        //string sql = " SELECT * FROM EX47BOLETT WHERE FK4776ANOREG = " + baixa.sAnoReg + " AND FK4776SEQREG = " + baixa.sSeqReg;
                        //SqlConnection conn = new SqlConnection(baixa.sStringConexao);
                        //conn.Open();
                        //SqlCommand cmdd = new SqlCommand(sql, conn);
                        //cmdd.ExecuteNonQuery(); // executa cmd
                        //SqlDataReader buscapag = cmdd.ExecuteReader();
                        //if (!buscapag.HasRows)
                        //{
                        baixa.bRollback = false;
                        //conn.Close();
                        ConnInsert classCon = new ConnInsert();
                        SqlConnection con = classCon.abrirConexao(baixa.sStringConexao);
                        SqlTransaction tranBaixa = con.BeginTransaction();

                        if (baixa.bGraduacao && !baixa.bPreInscricao)
                        {
                            InsereGraduacao insere = new InsereGraduacao();
                            insere.inserir(ref baixa, ref tranBaixa, ref con);
                        }
                        if (baixa.bRollback)
                        {
                            tranBaixa.Rollback();
                            con.Close();
                            classCon.fecharConexao();
                            classCon = null;
                        }
                        else
                        {
                            tranBaixa.Commit();
                            con.Close();
                            classCon.fecharConexao();
                            classCon = null;
                            tranBaixa = null;
                        }
                        //}
                    }
                }
            }
        }
    }
}
