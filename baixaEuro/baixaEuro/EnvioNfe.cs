using Microsoft.SqlServer.Server;
using Microsoft.VisualBasic;
using System;
using System.Data.SqlClient;

namespace baixaEuro
{
	public class EnvioNfe
	{
		public void notasBaixadas(ref DadosBoletoBaixa baixa)
		{
			string sql;
			if (baixa.bGraduacao)
			{
				 sql = " select PF47ANOBOL, PF47CODBOL from PF46BOLNEt" +
							 " inner join PF47BOLETT on  FK4746ANOBOL = PF46ANOBOL" +
							 " and FK4746BOLETA = PF46BOLETA" +
							 " and FK4737CPDALU = FK4637CPDALU" +
							 " where PF46ANOBOL = " + baixa.sAnoSemestre +
							 " and PF46BOLETA = " + baixa.sCodBol +
							 " and FK4637CPDALU = " + baixa.sCpdAluno;
            }
            else
            {
				sql = " select EX47ANOBOL AS PF47ANOBOL, EX47CODBOL AS PF47CODBOL from EX46BOLNEt" +
								" inner join EX47BOLETT on  FK4746ANOBOL = EX46ANOBOL" +
								" and FK4746BOLETA = EX46BOLETA" +
								" and FK4737CPDALU = FK4637CPDALU" +
								" where EX46ANOBOL = " + baixa.sAnoSemestre +
								" and EX46BOLETA = " + baixa.sCodBol +
								" and FK4637CPDALU = " + baixa.sCpdAluno;

			}

            if (baixa.bNfeinternetcaixa)
            {
                if (baixa.bGraduacao)
                {
					sql = " select PF47ANOBOL, PF47CODBOL " +
		                  " from PF47BOLETT " +
		                  " WHERE FK4737CPDALU = " + baixa.sCpdAluno +
		                  " AND PF76ANOREG = " + baixa.sAnoReg +
		                  " AND PF76SEQREG = " + baixa.sSeqReg;

                }
                else
                {
					sql = " select EX47ANOBOL AS PF47ANOBOL, EX47CODBOL AS PF47CODBOL " +
		                  " from EX47BOLETT " +
		                  " WHERE FK4737CPDALU = " + baixa.sCpdAluno +
						  " AND FK4776ANOREG = " + baixa.sAnoReg +
						  " AND FK4776SEQREG = " + baixa.sSeqReg;
				}

			}

			Connection cConn = new Connection();
			cConn.abrirConexao(baixa.sStringConexao);
			SqlDataReader retornoCodbol = cConn.buscaDados(sql);

			if (retornoCodbol.HasRows)
			{ int count = 0;
				while (retornoCodbol.Read())
				{ if (count > 0) {
						baixa.sNotasBaixadas = baixa.sNotasBaixadas + "-";
					}
					baixa.sNotasBaixadas = baixa.sNotasBaixadas + retornoCodbol["PF47CODBOL"].ToString()+"/"+ Strings.Right(retornoCodbol["PF47ANOBOL"].ToString(),2);
					count++;
				}
			}
			cConn.fecharConexao();
		}
		public void gravaDadosNota(ref DadosBoletoBaixa baixa)
		{
			baixa.sNotasBaixadas = "";
			if (baixa.bNegociacao)
			{
				notasBaixadas(ref baixa);
			}

			string cfop = "";
			string stringNota = "Data Source=CL2DB.CEUMA.EDU.BR;Initial Catalog=DBLOTENOTASFISCAIS;User ID=User_GrpCeuma;Password=G*pC&#m@08.19?";
			string ID_NOTA = "0";
			bool rollback = true;
			string idLote = "";

			if (!codigocidadeCadastrado(ref cfop, ref baixa))
			{
				return;
			}

			SqlConnection conBusca = new SqlConnection(stringNota);
			conBusca.Open();
			if (!jaGravadaNota(ref conBusca, baixa))
			{
				ConnInsert classCon = new ConnInsert();
				SqlConnection con = classCon.abrirConexao(stringNota);
				SqlTransaction tranNota = con.BeginTransaction();

				buscaCNpj(ref baixa, ref conBusca);
				criaLote(ref conBusca, ref con, ref tranNota, baixa, ref idLote);

				if (gravaCabecalho(ref conBusca, ref con, ref tranNota, baixa, ref ID_NOTA))
				{
					if (gravaItemNota(ref conBusca, ref con, ref tranNota, baixa, ref ID_NOTA, cfop))
					{
						Loteitem(ref con, ref tranNota, baixa, ref idLote, ref ID_NOTA);
						rollback = false;
					}
				}

				if (rollback)
				{
					tranNota.Rollback();
					con.Close();
					classCon.fecharConexao();
				}
				else
				{
					tranNota.Commit();
					con.Close();
					classCon.fecharConexao();
				}
			}
			else
			{
				conBusca.Close();
				return;
			}

			conBusca.Close();

			//if (countNota > 50)
			//{
			//	if (countEnviado == 50)
			//	{
			//		countNota = countNota - 50;
			//		countEnviado = 0;
			//		idLote = "0";
			//	}
			//}

			//string stringNota = "Data Source=CL3DB.CEUMA.EDU.BR;Initial Catalog=DBLOTENOTASFISCAIS;User ID=User_GrpCeuma;Password=G*pC&#m@08.19?";
			//string ID_NOTA = "0";
			//bool rollback = true;

			//SqlConnection conBusca = new SqlConnection(stringNota);
			//conBusca.Open();

			//if (!codigocidadeCadastrado(ref cfop, ref baixa))
			//{
			//	countNota --;
			//	return;
			//}

			//if (!jaGravadaNota(ref conBusca, baixa))
			//{
			//	ConnInsert classCon = new ConnInsert();
			//	SqlConnection con = classCon.abrirConexao(stringNota);
			//	SqlTransaction tranNota = con.BeginTransaction();

			//	if(idLote == "0")
			//	{
			//		criaLote(ref conBusca, ref con, ref tranNota, baixa, ref idLote);
			//	}

			//	if (gravaCabecalho(ref conBusca , ref con, ref tranNota, baixa, ref ID_NOTA))
			//	{
			//		if (gravaItemNota(ref conBusca, ref con, ref tranNota, baixa, ref ID_NOTA, cfop))
			//		{
			//			rollback = false;
			//			countEnviado++;
			//			itemLote( ref con, ref tranNota, baixa, ref idLote, ref ID_NOTA) ;
			//		}
			//	}
			//	if (rollback)
			//	{
			//		tranNota.Rollback();
			//		con.Close();
			//		classCon.fecharConexao();
			//		countNota--;
			//	}
			//	else
			//	{
			//		tranNota.Commit();
			//		con.Close();
			//		classCon.fecharConexao();
			//	}
			//}
			//else
			//{
			//	countNota--;
			//}
			//if (countEnviado == countNota)
			//{
			//	idLote = "0";
			//}
			//conBusca.Close();
		}
		public bool gravaCabecalho(ref SqlConnection conBusca, ref SqlConnection con, ref SqlTransaction tranNota, DadosBoletoBaixa baixa, ref string ID_NOTA)
		{
			SqlCommand cmd;
			bool retorno = false;

			//busca codigo do campus do aluno

			//string sqlCodCam = " SELECT DISTINCT '0'+ RIGHT(PF37MATALU,1) codCampus FROM PF37ALUNOT" +
			//				   " WHERE RIGHT(PF37MATALU,1) not in ('C','') " +
			//	               " AND PF37CPDALU ='" + baixa.sCpdAluno + "' ";


			//SqlConnection conexao = new SqlConnection(baixa.sStringConexao);
			//conexao.Open();
			//cmd = new SqlCommand(sqlCodCam, conexao);
			//cmd.ExecuteNonQuery(); // executa cmd
			//SqlDataReader rsCod = cmd.ExecuteReader();

			//if (rsCod.HasRows)
			//{
			//	rsCod.Read();
			//	baixa.sCodCam = rsCod["codCampus"].ToString();
			//	rsCod.Close();
			//	conexao.Close();
			//}
			//else
			//{
			//	rsCod.Close();
			//	conexao.Close();
			//	return retorno;
			//}

			//busca cnpj do Campus

			//string sqlCpf = " SELECT num_CNPJ FROM Prestador WHERE codCampus = '"+ baixa.sCodCam +"' ";
			//string cnpj = null;
			//cmd = new SqlCommand(sqlCpf, conBusca);
			//cmd.ExecuteNonQuery(); // executa cmd
			//SqlDataReader rsCnpj = cmd.ExecuteReader();

			//if (rsCnpj.HasRows)
			//{
			//	rsCnpj.Read();
			//	cnpj = rsCnpj["num_CNPJ"].ToString();
			//	rsCnpj.Close();
			//}
			//else
			//{
			//	rsCnpj.Close();
			//}

			string numNota;
			string sqlCabecalho;

			if (baixa.sCnpjNfe == "37174034000293")
			{
				SqlDataReader rtNota;
				numNota = " SELECT isnull(max(NumNota)+1,1) as NumNota FROM CabNota where  id > 6817 and fkCnpj = '" + baixa.sCnpjNfe + "' ";

				conBusca.Close();
				cmd = new SqlCommand(numNota, conBusca);
				conBusca.Open();
				cmd.ExecuteNonQuery(); // executa cmd
				rtNota = cmd.ExecuteReader();

				if (rtNota.HasRows)
				{
					rtNota.Read();
					numNota = rtNota["NumNota"].ToString();
					if (Int32.Parse(numNota) > 399)
					{
						numNota = " (SELECT isnull(max(NumNota)+1,1) as NumNota FROM CabNota where fkCnpj = '" + baixa.sCnpjNfe + "' ) ";
					}
				}
				conBusca.Close();
			}
			else
			{
				numNota = " (SELECT isnull(max(NumNota)+1,1) as NumNota FROM CabNota where fkCnpj = '" + baixa.sCnpjNfe + "' ) ";
			}

			sqlCabecalho = " INSERT INTO CabNota (NumNota , Serie , fkCpdAlu , fkCnpj , dhEmissao , ValorServico , vBaseCalculo , vIss , vPis," +
			" vDeducao, vOutro, vIssRer, CodUser, parcelas) " +
			" VALUES( " + numNota + " , '001' , '" + baixa.sCpdAluno + "' , '" + baixa.sCnpjNfe + "' , '" + baixa.sDataPagamento + "' , replace ('" + baixa.dValorPago + "',',','.') , " +
			" 0 , 0 , 0 , 0 , 0 , 0 , '5063', '" + baixa.sNotasBaixadas + "' ) ";

			cmd = null;
			cmd = con.CreateCommand();
			cmd.CommandText = sqlCabecalho;
			cmd.Transaction = tranNota;
			try
			{
				//ID_NOTA = Convert.ToString(cmd.ExecuteScalar());
				cmd.ExecuteNonQuery();
				cmd = null;

				cmd = con.CreateCommand();
				string sql = " SELECT Id FROM CabNota WHERE fkCpdAlu = " + baixa.sCpdAluno + " AND dhEmissao = '" + baixa.sDataPagamento + "' AND valorServico = replace ('" + baixa.dValorPago + "',',','.') ";
				cmd.CommandText = sql;
				cmd.Transaction = tranNota;
				ID_NOTA = Convert.ToString(cmd.ExecuteScalar());
				retorno = true;
				return retorno;
			}
			catch
			{
				return retorno;
			}
		}
		public bool gravaItemNota(ref SqlConnection conBusca, ref SqlConnection con, ref SqlTransaction tranNota, DadosBoletoBaixa baixa, ref string ID_NOTA, string cfop)
		{
			SqlCommand cmd;
			SqlDataReader rtProtudo;
			string sqlProduto = " SELECT * FROM Produtos where cod_Produto = '" + baixa.sCodProdutoNfe + "' ";
			conBusca.Close();
			cmd = new SqlCommand(sqlProduto, conBusca);
			conBusca.Open();
			cmd.ExecuteNonQuery(); // executa cmd
			rtProtudo = cmd.ExecuteReader();

			if (rtProtudo.HasRows)
			{
				rtProtudo.Read();

				string sqlItem = " INSERT INTO ItemNota (fk_id_CabNota , NumItem , CodProd , DescProd , EAN , NCM , CFOP , Unidade , vPRODUTO," +
								 " NossoNumero, qQuant, VlrUnit ) " +
								 " VALUES( " + ID_NOTA + " , 001 , " + rtProtudo["cod_Produto"].ToString() + " , '" + rtProtudo["desc_Produto"].ToString() + "' , NULL , '00' , '" + cfop + "' , 'UN' , replace('" + baixa.dValorPago + "',',','.') , '" + baixa.sNossoNumero + "', '1', replace('" + baixa.dValorPago + "',',','.')) ";

				rtProtudo.Close();

				cmd = con.CreateCommand();
				try
				{
					cmd.CommandText = sqlItem;
					cmd.Transaction = tranNota;
					cmd.ExecuteNonQuery();
					//ID_NOTA = Convert.ToInt32(cmd.ExecuteScalar());
					return true;
				}
				catch (Exception)
				{
					return false;
				}
			}
			else
			{
				return false;
			}
		}
		public bool jaGravadaNota(ref SqlConnection con, DadosBoletoBaixa baixa)
		{
			string sql = " select * from CabNota where fkCpdAlu = '" + baixa.sCpdAluno + "' and dhEmissao = '" + baixa.sDataPagamento + "' and ValorServico = replace ('" + baixa.dValorPago + "',',','.')";

			SqlCommand cmd = new SqlCommand(sql, con);
			cmd.ExecuteNonQuery(); // executa cmd
			SqlDataReader buscaNota = cmd.ExecuteReader();
			if (buscaNota.HasRows)
			{
				buscaNota.Close();
				return true;
			}
			else
			{
				buscaNota.Close();
				return false;
			}
		}
		public void criaLote(ref SqlConnection conBusca, ref SqlConnection con, ref SqlTransaction tranNota, DadosBoletoBaixa baixa, ref string idLote)
		{
			string sqlBusca;
			string sqlInsert = "";
			bool insert = false;
			string quant;
			sqlBusca = " select ISNULL(MAX(NumLote),0) as numLote from lote where cnpj_prestador = '" + baixa.sCnpjNfe + "' AND STATUS IS NULL ";

			SqlCommand cmd;
			SqlDataReader rtNumLote;

			cmd = new SqlCommand(sqlBusca, conBusca);
			cmd.ExecuteNonQuery(); // executa cmd
			rtNumLote = cmd.ExecuteReader();
			rtNumLote.Read();
			if (rtNumLote["numLote"].ToString() != "0")
			{
				idLote = rtNumLote["numLote"].ToString();

				sqlBusca = " select count(0) as quant from CabNota cb " +
						   " inner join LoteNota lt on lt.fkNumNota = cb.Id" +
						   " and fkCnpj ='" + baixa.sCnpjNfe + "' " +
						   " and lt.fkNumLote = '" + idLote + "'";

				rtNumLote.Close();
				cmd = new SqlCommand(sqlBusca, conBusca);
				cmd.ExecuteNonQuery(); // executa cmd
				rtNumLote = cmd.ExecuteReader();
				int x;

				if (rtNumLote.HasRows)
				{

					rtNumLote.Read();
					quant = rtNumLote["quant"].ToString();
					if (Int32.Parse(quant) < 50)
					{
						return;
					}
					else
					{
						x = Int32.Parse(idLote) + 1;
						idLote = x.ToString();
						insert = true;
					}
				}
				else
				{
					return;
					//x = Int32.Parse(idLote) + 1;
					//idLote = x.ToString();
					//insert = true;
				}
				rtNumLote.Close();
			}
			else
			{
				sqlBusca = " select isnull(MAX(NumLote)+1,1) as loteid from lote where cnpj_prestador = '" + baixa.sCnpjNfe + "'";

				rtNumLote.Close();
				cmd = new SqlCommand(sqlBusca, conBusca);
				cmd.ExecuteNonQuery(); // executa cmd
				rtNumLote = cmd.ExecuteReader();

				rtNumLote.Read();
				idLote = rtNumLote["loteid"].ToString();
				rtNumLote.Close();
				insert = true;
			}

			if (insert)
			{
				sqlInsert = "INSERT INTO Lote (NumLote, cnpj_prestador ) VALUES ('" + idLote + "','" + baixa.sCnpjNfe + "')";

				cmd = con.CreateCommand();

				try
				{
					cmd.CommandText = sqlInsert;
					cmd.Transaction = tranNota;
					cmd.ExecuteNonQuery();

				}
				catch (Exception)
				{
					Console.WriteLine("Erro ao Criar Lote");
				}
			}
		}
		public void Loteitem(ref SqlConnection con, ref SqlTransaction tranNota, DadosBoletoBaixa baixa, ref string idLote, ref string ID_NOTA)
		{
			string sqlInsert = "INSERT INTO LoteNota (fkNumLote, fkNumNota, CodUser) VALUES (" + idLote + "," + ID_NOTA + ", 5063)";
			SqlCommand cmd = con.CreateCommand();
			try
			{
				cmd.CommandText = sqlInsert;
				cmd.Transaction = tranNota;
				cmd.ExecuteNonQuery();
				//ID_NOTA = Convert.ToInt32(cmd.ExecuteScalar());
			}
			catch (Exception)
			{
				Console.WriteLine("Erro ao Criar Lote");
			}

		}
		public bool codigocidadeCadastrado(ref string cfop, ref DadosBoletoBaixa baixa)
		{

			bool retorno = false;
			string sql;
			if (baixa.bGraduacao)
			{
				 sql = " SELECT ISNULL(CONT.pfa9codmun, '0') CODMUN, PFA9CPF as CPF FROM PF37ALUNOT ALU "
						   + " INNER JOIN PFA9CONALT CONT ON  CONT.PFA9CPF = ALU.FK37A9CPF "
						   + " WHERE ALU.PF37CPDALU =" + baixa.sCpdAluno;
            }
            else 
			{
			    sql = " SELECT ISNULL(CONT.EXa9codmun, '0') CODMUN, EXA9CPF as CPF FROM EX37ALUNOT ALU "
			               + " INNER JOIN EXA9CONALT CONT ON  CONT.EXA9CPF = ALU.FK37A9CPF "
			               + " WHERE ALU.EX37CPDALU =" + baixa.sCpdAluno;


			}

			SqlConnection conBusca = new SqlConnection(baixa.sStringConexao);
			conBusca.Open();
			SqlCommand cmd = new SqlCommand(sql, conBusca);
			cmd.ExecuteNonQuery(); // executa cmd
			SqlDataReader rt = cmd.ExecuteReader();

			if (rt.HasRows)
			{
				rt.Read();
				if (rt["CODMUN"].ToString() != "")
				{
					if (rt["CODMUN"].ToString() != "0")
					{

						if (Strings.Left(rt["CODMUN"].ToString(), 2) == "53")
						{
							cfop = "5933";
						}
						else
						{
							cfop = "6933";
						}
						retorno = true;
					}
				}
				else
				{
					string cpf = rt["CPF"].ToString();
					gravarCodigoMunicipio(cpf, baixa);
					cfop = "5933";
				}
			}
			conBusca.Close();
			rt.Close();
			return retorno;
		}
		public void gravarCodigoMunicipio(string cpf, DadosBoletoBaixa baixa)
		{
			string sql = "";
			if (baixa.bGraduacao)
			{
				sql = " UPDATE PFA9CONALT "
					+ " SET PFA9CODMUN = '5300108' "
					+ " WHERE PFA9CPF = '" + cpf + "'";

			}
			else
			{
				sql = " UPDATE EXA9CONALT "
					 + " SET EXA9CODMUN = '5300108' "
					 + " WHERE EXA9CPF = '" + cpf + "'";
			}

			ConnInsert classCon = new ConnInsert();
			SqlConnection con = classCon.abrirConexao(baixa.sStringConexao);
			SqlTransaction tran = con.BeginTransaction();
			SqlCommand cmd = con.CreateCommand();
			try
			{
				cmd.CommandText = sql;
				cmd.Transaction = tran;
				cmd.ExecuteNonQuery();
				tran.Commit();
			}
			catch
			{
				tran.Rollback();

			}
			finally
			{
				tran.Dispose();
				con.Close();
				cmd.Dispose();
				classCon.fecharConexao();
			}

		}
		public void buscaCNpj(ref DadosBoletoBaixa baixa, ref SqlConnection conBusca)
		{
			SqlCommand cmd;
			//busca codigo do campus do aluno
			string sqlCodCam;

			if (baixa.bGraduacao)
            {
				sqlCodCam = " SELECT DISTINCT '0'+ RIGHT(PF37MATALU,1) codCampus FROM PF37ALUNOT" +
				   " WHERE RIGHT(PF37MATALU,1) not in ('C','') " +
				   " AND PF37CPDALU ='" + baixa.sCpdAluno + "' ";

			}
            else
            {
				 sqlCodCam = " SELECT DISTINCT '0'+ RIGHT(EX37MATALU,1) codCampus FROM EX37ALUNOT" +
				   " WHERE RIGHT(EX37MATALU,1) not in ('C','') " +
				   " AND EX37CPDALU ='" + baixa.sCpdAluno + "' ";
			}

			SqlConnection conexao = new SqlConnection(baixa.sStringConexao);
			conexao.Open();
			cmd = new SqlCommand(sqlCodCam, conexao);
			cmd.ExecuteNonQuery(); // executa cmd
			SqlDataReader rsCod = cmd.ExecuteReader();

			if (rsCod.HasRows)
			{
				rsCod.Read();
				baixa.sCodCam = rsCod["codCampus"].ToString();
				rsCod.Close();
				conexao.Close();
			}
			else
			{
				rsCod.Close();
				conexao.Close();
			}

			string sqlCpf = " SELECT num_CNPJ FROM Prestador WHERE codCampus = '" + baixa.sCodCam + "' ";
			cmd = new SqlCommand(sqlCpf, conBusca);
			cmd.ExecuteNonQuery(); // executa cmd
			SqlDataReader rsCnpj = cmd.ExecuteReader();

			if (rsCnpj.HasRows)
			{
				rsCnpj.Read();
				baixa.sCnpjNfe = rsCnpj["num_CNPJ"].ToString();
				rsCnpj.Close();
			}
			else
			{
				rsCnpj.Close();
			}
		}
	}
}
