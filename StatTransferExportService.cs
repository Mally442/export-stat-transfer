using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using BLL.BPC.Analytics.ExportProviders;
using BLL.BPC.Analytics.Models;
using BLL.BPC.Analytics.Models.Columns;
using BLL.BPC.Models.Responses;
using BLL.DAL;
using BLL.DAL.Lookup;
using BLL.Framework.Helpers;
using ICSharpCode.SharpZipLib.Core;
using ICSharpCode.SharpZipLib.Zip;
using System.Data;
using BLL.BPC.Account;
using BLL.BPC.GPS;
using System.Configuration;

namespace BLL.BPC.Analytics
{
	/// <summary>
	/// Service used to export analytics data to Stat Transfer and CSV formats.
	/// </summary>
	public class StatTransferExportService
	{
		private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

		// The analytics export format
		private ExportFormat format;

		private string fileName;

		// Format flags
		private bool isFormatCSV;
		private bool isFormatStatTransfer;
		private bool isFormatEach;
		private bool isFormatSeparate;
		private bool isFormatRows;
		private bool isFormatColumns;
		private bool shortNamingOption;
		private bool useSingleValues;
		private bool enforceEncapsulation;

		private IFilter analyticsFilter;
		private SurveyColumnModel columnFilter;
		private Dictionary<string, string> columnHeaderLookup;
		private Dictionary<string, bool> visibleColumnLookup;
		private IEnumerable<MaxQuestionInstance> responseInstances;
		private SubmissionSummary submissionSummary;
		private Dictionary<int, QuestionEntity> questionLookup;
		private List<StatFile> StatFiles;
		private List<StructuralFile> StructuralFiles;

		/// <summary>
		/// Export analytics data to a Stat Transfer or CSV format.
		/// </summary>
		/// <param name="format">The analytics export format.</param>
		/// <param name="naming">The analytics export naming.</param>
		/// <param name="singleOutput">The single output.</param>
		/// <param name="filter">The filter.</param>
		/// <param name="surveyColumnModel">The survey column model.</param>
		/// <param name="outputStream">The output stream.</param>
		/// <param name="fileName">Name of the file.</param>
		/// <param name="workflowId">The workflow id.</param>
		/// <exception cref="System.NotSupportedException">The export format is not supported by the Stat Transfer data export service</exception>
		public void ExportData(ExportFormat format, ExportNaming naming, ExportSingleOutput singleOutput, IFilter filter, SurveyColumnModel surveyColumnModel, Stream outputStream, string fileName, string workflowId = null)
		{
			this.format = format;
			this.fileName = fileName;

			// Initialise formatting flags
			this.isFormatCSV = this.format == ExportFormat.CSVEach || this.format == ExportFormat.CSVSeparate || this.format == ExportFormat.CSVRows || this.format == ExportFormat.CSVColumns || this.format == ExportFormat.CSVFlatLong;
			this.isFormatStatTransfer = this.format == ExportFormat.StatTransferEach || this.format == ExportFormat.StatTransferSeparate || this.format == ExportFormat.StatTransferRows || this.format == ExportFormat.StatTransferColumns || this.format == ExportFormat.StatTransferLong;
			this.isFormatEach = this.format == ExportFormat.CSVEach || this.format == ExportFormat.StatTransferEach;
			this.isFormatSeparate = this.format == ExportFormat.CSVSeparate || this.format == ExportFormat.StatTransferSeparate;
			this.isFormatRows = this.format == ExportFormat.CSVRows || this.format == ExportFormat.StatTransferRows;
			this.isFormatColumns = this.format == ExportFormat.CSVColumns || this.format == ExportFormat.StatTransferColumns || this.format == ExportFormat.CSVFlatLong || this.format == ExportFormat.StatTransferLong;
			this.shortNamingOption = naming != ExportNaming.Long;
			this.useSingleValues = singleOutput != ExportSingleOutput.Labels;
			this.enforceEncapsulation = this.isFormatCSV;

			this.analyticsFilter = filter;
			this.columnFilter = surveyColumnModel;
			this.columnHeaderLookup = ColumnHeaderHelper.GenerateLookup(this.columnFilter);
			this.visibleColumnLookup = VisibleColumnHelper.GenerateLookup(this.columnFilter);

			if (this.isFormatColumns)
			{
				// Get the max question instances to be used for instance specific columns
				var responseController = new ResponseController();
				this.responseInstances = responseController.GetMaxInstanceNumberForQuestionsAskedMoreThanOnce(this.analyticsFilter.SurveyId);
			}

			// Retrieve the first round of data
			var analyticsService = AnalyticsServiceFactory.GetAnalyticsService();
			analyticsService.SqlTimeoutSeconds = 300;

			int page = 0;
			var pageSize = ConfigurationManager.AppSettings["EXPORT_SOLR_QUERY_PAGE_SIZE"].ToInt();
			((SubmissionFilter)this.analyticsFilter).SetupPager(page, pageSize);
			((SubmissionFilter)this.analyticsFilter).SetupSorting(columnFilter.Sort);

			this.submissionSummary = analyticsService.RetrieveData(ContextProvider.GetCurrentAccountID(), this.analyticsFilter, out this.questionLookup);
			bool submissionsRemaining = this.submissionSummary.Submissions.Count() > 0;

			// Create temp directory
			string id = workflowId ?? Guid.NewGuid().ToString();
			var tempDirectory = Path.Combine(System.AppDomain.CurrentDomain.BaseDirectory, "temp\\{0}".FormatText(id));
			if (!Directory.Exists(tempDirectory))
			{
				Directory.CreateDirectory(tempDirectory);
			}

			// Determines the stat files to be created
			this.DetermineStatFiles(tempDirectory);

			// Initialise file stream writers
			this.StatFiles.ForEach(statFile => statFile.Writer = new StreamWriter(statFile.FilePath, false, Encoding.UTF8));

			// Write the headings of the data files
			this.WriteDataHeadings();

			while (submissionsRemaining)
			{
				// Write the data of the data files
				foreach (var submission in this.submissionSummary.Submissions)
				{
					// Write the meta data of the data files
					foreach (var statFile in this.StatFiles)
					{
						statFile.FirstStatVariable = true;

						foreach (var statVariable in statFile.StatVariables.Where(v => v.Question == null))
						{
							if (statFile.FirstStatVariable)
							{
								statFile.FirstStatVariable = false;
							}
							else
							{
								statFile.Writer.Write(",");
							}

							this.WriteMetaData(statFile.Writer, submission, statVariable, 0, statFile.RepeatingQuestionId);
						}
					}

					// Write the question data of the data files
					foreach (var statFile in this.StatFiles)
					{
						// Table for storing repeating data if needed
						var reapeatingDataTable = new DataTable();
						foreach (var statVariable in statFile.StatVariables.Where(v => v.Question != null))
						{
							if (statFile.FirstStatVariable)
							{
								statFile.FirstStatVariable = false;
							}
							else
							{
								statFile.Writer.Write(",");
							}

							FieldResponseSummary fieldResponse;
							if (!submission.FieldResponseLookup.TryGetValue(statVariable.Question.id, out fieldResponse))
							{
								continue;
							}

							if (this.isFormatColumns || !statFile.IsRepeating)
							{
								this.WriteQuestionData(statFile.Writer, fieldResponse.Responses, statVariable, statVariable.Instance);
							}
							else
							{
								// Repeating response data needs to be exported in multiple rows after the first instance
								this.WriteQuestionData(statFile.Writer, fieldResponse.Responses, statVariable, 0);

								// Add repeating response data, with an instance greater than 0, to a table to be written later
								foreach (var repeatingResponse in fieldResponse.Responses.Where(r => r.Instance > 0))
								{
									if (repeatingResponse == null || repeatingResponse.Value.IsNullOrEmpty())
									{
										continue;
									}

									if (!reapeatingDataTable.Columns.Contains(statVariable.Name))
									{
										reapeatingDataTable.Columns.Add(statVariable.Name, typeof(Models.Response));
									}
									while (reapeatingDataTable.Rows.Count < repeatingResponse.Instance)
									{
										reapeatingDataTable.Rows.Add(reapeatingDataTable.NewRow());
									}
									reapeatingDataTable.Rows[repeatingResponse.Instance - 1][statVariable.Name] = repeatingResponse;
								}
							}
						}

						// Write repeating response data
						for (int i = 0; i < reapeatingDataTable.Rows.Count; i++)
						{
							statFile.Writer.WriteLine();
							statFile.FirstStatVariable = true;

							foreach (var statVariable in statFile.StatVariables.Where(v => v.Question == null))
							{
								if (statFile.FirstStatVariable)
								{
									statFile.FirstStatVariable = false;
								}
								else
								{
									statFile.Writer.Write(",");
								}

								this.WriteMetaData(statFile.Writer, submission, statVariable, i + 1, statFile.RepeatingQuestionId);
							}

							foreach (var statVariable in statFile.StatVariables.Where(v => v.Question != null))
							{
								if (statFile.FirstStatVariable)
								{
									statFile.FirstStatVariable = false;
								}
								else
								{
									statFile.Writer.Write(",");
								}

								if (reapeatingDataTable.Columns.Contains(statVariable.Name))
								{
									var repeatingResponse = reapeatingDataTable.Rows[i][statVariable.Name] as Models.Response;
									this.WriteQuestionData(statFile.Writer, repeatingResponse.GenerateEnumerable(), statVariable);
								}
								else if (!statVariable.Question.SurveySection.IsRepeating)
								{
									// Write duplicate data for fixed non-repeating question in repeating section file
									FieldResponseSummary fieldResponse;
									if (!submission.FieldResponseLookup.TryGetValue(statVariable.Question.id, out fieldResponse))
									{
										continue;
									}

									this.WriteQuestionData(statFile.Writer, fieldResponse.Responses, statVariable);
								}
							}
						}
					}

					this.StatFiles.ForEach(statFile => statFile.Writer.WriteLine());
				}
				this.StatFiles.ForEach(statFile => statFile.Writer.Flush());

				// Calculate whether another round of data is to be written
				if (this.submissionSummary.TotalSubmissionCount > (page + 1) * pageSize)
				{
					page++;

					if (ConfigurationManager.AppSettings["ANALYTICS_EXPORT_MEMORY_USAGE_LOGGING"].ToBool() && page % 2 == 0)
					{
						var memoryUsage = Environment.WorkingSet;
						log.Info("Memory usage after writing {0} rows of a Stat Transfer data file: {1} bytes ({2:#.##} MB)".FormatText(page * pageSize, memoryUsage, memoryUsage / 1048576));
					}

					((SubmissionFilter)this.analyticsFilter).SetupPager(page, pageSize);

					// Retrieve the next page of submissions and remove those that were in the previous page
					var previousIds = this.submissionSummary.Submissions.Select(submission => submission.Id);
					this.submissionSummary = analyticsService.RetrieveData(ContextProvider.GetCurrentAccountID(), this.analyticsFilter, out this.questionLookup);
					this.submissionSummary.Submissions = this.submissionSummary.Submissions.Where(submission => !previousIds.Contains(submission.Id)).ToList();
				}
				else
				{
					submissionsRemaining = false;
				}
			}
			this.StatFiles.ForEach(statFile => statFile.Writer.Flush());
			this.StatFiles.ForEach(statFile => statFile.Writer.Close());
			this.StatFiles.ForEach(statFile => statFile.Writer.Dispose());

			this.StructuralFiles = new List<StructuralFile>();
			if (this.isFormatCSV)
			{
				// Write the code book file
				var codebookFile = new StructuralFile
				{
					FilePath = Path.Combine(tempDirectory, "Code Book.csv")
				};
				codebookFile.FileWriter = new StreamWriter(codebookFile.FilePath, false, Encoding.UTF8);
				this.StructuralFiles.Add(codebookFile);
				this.WriteCodebook(codebookFile.FileWriter);

				// Write the questions file to comply with the legacy CSVEach format
				var questionsFile = new StructuralFile
				{
					FilePath = Path.Combine(tempDirectory, "Questions.csv")
				};
				questionsFile.FileWriter = new StreamWriter(questionsFile.FilePath, false, Encoding.UTF8);
				this.StructuralFiles.Add(questionsFile);
				this.WriteQuestions(questionsFile.FileWriter);
			}
			else if (this.isFormatStatTransfer)
			{
				// Write the schema file(s)
				foreach (var statFile in this.StatFiles)
				{
					var schemaFile = new StructuralFile
					{
						FilePath = Path.Combine(tempDirectory, "{0}.stsd".FormatText(Path.GetFileNameWithoutExtension(statFile.FilePath)))
					};
					schemaFile.FileWriter = new StreamWriter(schemaFile.FilePath, false, new UTF8Encoding(false));
					this.StructuralFiles.Add(schemaFile);
					this.WriteSchema(schemaFile.FileWriter, statFile);
				}
			}
			else
			{
				throw new NotSupportedException("The export format '{0}' is not supported by the Stat Transfer data export service".FormatText(this.format));
			}
			this.StructuralFiles.ForEach(structuralFile => structuralFile.FileWriter.Flush());
			this.StructuralFiles.ForEach(structuralFile => structuralFile.FileWriter.Close());
			this.StructuralFiles.ForEach(structuralFile => structuralFile.FileWriter.Dispose());

			// Zip the data and structural files
			var zipStream = new ZipOutputStream(outputStream);
			zipStream.SetLevel(9);

			foreach (var statFile in this.StatFiles)
			{
				var statZipEntry = new ZipEntry(Path.GetFileName(statFile.FilePath))
				{
					DateTime = DateTime.Now,
					IsUnicodeText = true
				};
				zipStream.PutNextEntry(statZipEntry);
				var statFileStream = new FileStream(statFile.FilePath, FileMode.Open);
				StreamUtils.Copy(statFileStream, zipStream, new byte[4096]);
				zipStream.CloseEntry();
				statFileStream.Close();
				statFileStream.Dispose();
			}

			foreach (var structuralFile in this.StructuralFiles)
			{
				var structuralZipEntry = new ZipEntry(Path.GetFileName(structuralFile.FilePath))
				{
					DateTime = DateTime.Now,
					IsUnicodeText = true
				};
				zipStream.PutNextEntry(structuralZipEntry);
				var structuralFileStream = new FileStream(structuralFile.FilePath, FileMode.Open);
				StreamUtils.Copy(structuralFileStream, zipStream, new byte[4096]);
				zipStream.CloseEntry();
				structuralFileStream.Close();
				structuralFileStream.Dispose();
			}

			zipStream.IsStreamOwner = false;
			zipStream.Close();
			zipStream.Dispose();

			// Delete the data and structural files now that they are contained within the zip file
			this.StatFiles.ForEach(statFile => File.Delete(statFile.FilePath));
			this.StructuralFiles.ForEach(structuralFile => File.Delete(structuralFile.FilePath));

			if (workflowId.IsNullOrEmpty())
			{
				// If the data was exported without a workflow, the temp directory can also be deleted
				Directory.Delete(tempDirectory);
			}
		}

		/// <summary>
		/// Determines the stat files to be created.
		/// </summary>
		/// <param name="tempDirectory">The temporary directory.</param>
		private void DetermineStatFiles(string tempDirectory)
		{
			string dataFileExtension = this.isFormatStatTransfer ? "dat" : "csv";

			StatSection currrentSection;
			this.StatFiles = new List<StatFile>();

			if (this.isFormatEach)
			{
				// Add separate submissions meta data file since response data will be in individual section files
				var submissionsFile = new StatFile
										{
											Name = "Submissions",
											FilePath = Path.Combine(tempDirectory, "Submissions.{0}".FormatText(dataFileExtension)),
											StatVariables = new List<StatVariable>()
										};
				submissionsFile.StatVariables.AddRange(this.DetermineStatVariablesForMetaData(ColumnModel.SUBMISSION_ID));
				submissionsFile.StatVariables.AddRange(this.DetermineStatVariablesForMetaData(ColumnModel.FIELDWORKER_NAME));
				submissionsFile.StatVariables.AddRange(this.DetermineStatVariablesForMetaData(ColumnModel.DEVICE_NAME));
				submissionsFile.StatVariables.AddRange(this.DetermineStatVariablesForMetaData(ColumnModel.RECEIVED_DATE));
				submissionsFile.StatVariables.AddRange(this.DetermineStatVariablesForMetaData(ColumnModel.START_DATE));
				submissionsFile.StatVariables.AddRange(this.DetermineStatVariablesForMetaData(ColumnModel.END_DATE));
				submissionsFile.StatVariables.AddRange(this.DetermineStatVariablesForMetaData(ColumnModel.DURATION_IN_SECONDS));
				submissionsFile.StatVariables.AddRange(this.DetermineStatVariablesForMetaData(ColumnModel.LATITUDE));
				submissionsFile.StatVariables.AddRange(this.DetermineStatVariablesForMetaData(ColumnModel.LONGITUDE));
				submissionsFile.StatVariables.AddRange(this.DetermineStatVariablesForMetaData(ColumnModel.LANGUAGE));
				submissionsFile.StatVariables.AddRange(this.DetermineStatVariablesForMetaData(ColumnModel.SURVEY_VERSION));
				submissionsFile.StatVariables.AddRange(this.DetermineStatVariablesForMetaData(ColumnModel.MODIFIED_BY));
				submissionsFile.StatVariables.AddRange(this.DetermineStatVariablesForMetaData(ColumnModel.MODIFIED_ON));
				submissionsFile.StatVariables.AddRange(this.DetermineStatVariablesForMetaData(ColumnModel.IS_COMPLETE));

				// Add fixed question columns
				if (this.columnFilter != null && this.columnFilter.FixedColumns.Count() > 0)
				{
					foreach (var fixedColumn in this.columnFilter.FixedColumns.Where(x => x.DataIndex.StartsWith(ColumnModel.QUESTION_PREFIX)))
					{
						int questionId = fixedColumn.DataIndex.Substring(ColumnModel.QUESTION_PREFIX.Length).ToInt();
						QuestionEntity question = this.questionLookup[questionId];
						var sanitisedQuestionName = SanitiseString(question.name);
						var validQuestionName = Regex.IsMatch(sanitisedQuestionName, "^[a-zA-Z_].*") ? sanitisedQuestionName : "_{0}".FormatText(sanitisedQuestionName);
						submissionsFile.StatVariables.AddRange(this.DetermineStatVariablesForQuestion(question, validQuestionName));
					}
				}

				this.StatFiles.Add(submissionsFile);
			}
			else
			{
				// Add merged responses file which includes meta data
				var responsesFile = new StatFile
				{
					Name = "Responses",
					StatVariables = new List<StatVariable>()
				};
				if (this.isFormatSeparate)
				{
					responsesFile.FilePath = Path.Combine(tempDirectory, "Non-Repeating Responses.{0}".FormatText(dataFileExtension));
				}
				else
				{
					if (AccountProperties.LegacyCSVExportFilenamesEnabled)
					{
						responsesFile.FilePath = Path.Combine(tempDirectory, "{0}.{1}".FormatText(Path.GetFileNameWithoutExtension(this.fileName), dataFileExtension));
					}
					else
					{
						responsesFile.FilePath = Path.Combine(tempDirectory, "Responses.{0}".FormatText(dataFileExtension));
					}
				}
				responsesFile.StatVariables.AddRange(this.DetermineStatVariablesForMetaData(ColumnModel.SUBMISSION_ID));
				responsesFile.StatVariables.AddRange(this.DetermineStatVariablesForMetaData(ColumnModel.FIELDWORKER_NAME));
				responsesFile.StatVariables.AddRange(this.DetermineStatVariablesForMetaData(ColumnModel.DEVICE_NAME));
				responsesFile.StatVariables.AddRange(this.DetermineStatVariablesForMetaData(ColumnModel.RECEIVED_DATE));
				responsesFile.StatVariables.AddRange(this.DetermineStatVariablesForMetaData(ColumnModel.START_DATE));
				responsesFile.StatVariables.AddRange(this.DetermineStatVariablesForMetaData(ColumnModel.END_DATE));
				responsesFile.StatVariables.AddRange(this.DetermineStatVariablesForMetaData(ColumnModel.DURATION_IN_SECONDS));
				responsesFile.StatVariables.AddRange(this.DetermineStatVariablesForMetaData(ColumnModel.LATITUDE));
				responsesFile.StatVariables.AddRange(this.DetermineStatVariablesForMetaData(ColumnModel.LONGITUDE));
				responsesFile.StatVariables.AddRange(this.DetermineStatVariablesForMetaData(ColumnModel.LANGUAGE));
				responsesFile.StatVariables.AddRange(this.DetermineStatVariablesForMetaData(ColumnModel.SURVEY_VERSION));
				responsesFile.StatVariables.AddRange(this.DetermineStatVariablesForMetaData(ColumnModel.MODIFIED_BY));
				responsesFile.StatVariables.AddRange(this.DetermineStatVariablesForMetaData(ColumnModel.MODIFIED_ON));
				responsesFile.StatVariables.AddRange(this.DetermineStatVariablesForMetaData(ColumnModel.IS_COMPLETE));

				// Add question columns to the merged responses file
				currrentSection = new StatSection
									{
										Id = -1
									};
				foreach (var question in this.questionLookup)
				{
					if (!this.visibleColumnLookup.ContainsKey("Question_{0}".FormatText(question.Value.id)) || !question.Value.section_id.HasValue)
					{
						continue;
					}

					if (question.Value.section_id.Value != currrentSection.Id)
					{
						// We are dealing with a new section
						currrentSection = new StatSection
						{
							Id = question.Value.section_id.Value,
							IsRepeating = question.Value.SurveySection.IsRepeating
						};
					}

					// The question name needs to be sanitised and checked if it is valid for the Stat Transfer format
					var sanitisedQuestionName = SanitiseString(question.Value.name);
					var validQuestionName = Regex.IsMatch(sanitisedQuestionName, "^[a-zA-Z_].*") ? sanitisedQuestionName : "_{0}".FormatText(sanitisedQuestionName);

					if (currrentSection.IsRepeating)
					{
						if (this.isFormatSeparate)
						{
							// Repeating response data will be in individual section files
							continue;
						}

						if (this.isFormatRows)
						{
							// Repeating response data will be in additional rows so there is no need for instance specific columns
							responsesFile.StatVariables.AddRange(this.DetermineStatVariablesForQuestion(question.Value, validQuestionName));
							responsesFile.IsRepeating = true;
						}
						else
						{
							if (question.Value.SurveySection.IsRepeating)
							{
								// Repeating response data will be in additional columns so we need instance specific columns
								MaxQuestionInstance maxQuestionInstance = this.responseInstances.SingleOrDefault(r => r.QuestionId == question.Value.id);
								int maxInstance = maxQuestionInstance == null ? 0 : maxQuestionInstance.MaxInstance;
								for (int i = 0; i <= maxInstance; i++)
								{
									responsesFile.StatVariables.AddRange(this.DetermineStatVariablesForQuestion(question.Value, validQuestionName, i));
								}
							}
							else
							{
								responsesFile.StatVariables.AddRange(this.DetermineStatVariablesForQuestion(question.Value, validQuestionName));
							}
						}
					}
					else
					{
						responsesFile.StatVariables.AddRange(this.DetermineStatVariablesForQuestion(question.Value, validQuestionName));
					}
				}

				this.StatFiles.Add(responsesFile);
			}

			if (!(this.isFormatEach || isFormatSeparate))
			{
				return;
			}

			// Add section file(s)
			StatFile sectionFile = null;
			currrentSection = new StatSection
								{
									Id = -1
								};
			foreach (var question in this.questionLookup)
			{
				if (!this.visibleColumnLookup.ContainsKey("Question_{0}".FormatText(question.Value.id)) || !question.Value.section_id.HasValue)
				{
					continue;
				}

				if (question.Value.section_id.Value != currrentSection.Id)
				{
					// We are dealing with a new section
					if (currrentSection.Id != -1 && sectionFile != null)
					{
						this.StatFiles.Add(sectionFile);
					}

					currrentSection = new StatSection
					{
						Id = question.Value.section_id.Value,
						IsRepeating = question.Value.SurveySection.IsRepeating
					};

					if (this.isFormatSeparate && !currrentSection.IsRepeating)
					{
						// Only repeating response data to be in individual section files for this format
						sectionFile = null;
						continue;
					}

					sectionFile = new StatFile
					{
						Name = "{0}_{1}".FormatText(question.Value.SurveySection.displayorder + 1, question.Value.SurveySection.title.RegexReplace(@"[^a-zA-Z0-9 _-]", "").Replace(' ', '_')),
						FilePath = Path.Combine(tempDirectory, "{0} {1}.{2}".FormatText(question.Value.SurveySection.displayorder + 1, question.Value.SurveySection.title.RegexReplace(@"[\\/:]", "_"), dataFileExtension)),
						StatVariables = new List<StatVariable>(),
						SectionId = currrentSection.Id,
						IsRepeating = currrentSection.IsRepeating,
						RepeatingQuestionId = question.Value.SurveySection.repeatquestion_id
					};
					sectionFile.StatVariables.AddRange(this.DetermineStatVariablesForMetaData(ColumnModel.SUBMISSION_ID));
					sectionFile.StatVariables.AddRange(this.DetermineStatVariablesForMetaData(ColumnModel.FIELDWORKER_NAME));
					if (currrentSection.IsRepeating)
					{
						sectionFile.StatVariables.AddRange(this.DetermineStatVariablesForMetaData(ColumnModel.SECTION_INDEX));
					}
					// Add fixed columns
					if (this.columnFilter != null && this.columnFilter.FixedColumns.Count() > 0)
					{
						foreach (var fixedColumn in this.columnFilter.FixedColumns)
						{
							// The fixed column is a question column
							if (fixedColumn.DataIndex.StartsWith(ColumnModel.QUESTION_PREFIX))
							{
								int questionId = fixedColumn.DataIndex.Substring(ColumnModel.QUESTION_PREFIX.Length).ToInt();
								QuestionEntity fixedQuestion = this.questionLookup[questionId];
								if (currrentSection.Id != fixedQuestion.SurveySection.id)
								{
									// Don't add fixed questions from the same section
									var sanitisedFixedQuestionName = SanitiseString(fixedQuestion.name);
									var validFixedQuestionName = Regex.IsMatch(sanitisedFixedQuestionName, "^[a-zA-Z_].*") ? sanitisedFixedQuestionName : "_{0}".FormatText(sanitisedFixedQuestionName);
									sectionFile.StatVariables.AddRange(this.DetermineStatVariablesForQuestion(fixedQuestion, validFixedQuestionName));
								}
							}
							// The fixed column is a meta column
							else
							{
								if (fixedColumn.DataIndex != ColumnModel.SUBMISSION_ID && fixedColumn.DataIndex != ColumnModel.FIELDWORKER_NAME)
								{
									sectionFile.StatVariables.AddRange(this.DetermineStatVariablesForMetaData(fixedColumn.DataIndex));
								}
							}
						}
					}
				}

				if (this.isFormatSeparate && !currrentSection.IsRepeating)
				{
					// Only repeating response data to be in individual section files for this format
					continue;
				}

				// The question name needs to be sanitised and checked if it is valid for the Stat Transfer format
				var sanitisedQuestionName = SanitiseString(question.Value.name);
				var validQuestionName = Regex.IsMatch(sanitisedQuestionName, "^[a-zA-Z_].*") ? sanitisedQuestionName : "_{0}".FormatText(sanitisedQuestionName);

				sectionFile.StatVariables.AddRange(this.DetermineStatVariablesForQuestion(question.Value, validQuestionName));
			}
			if (sectionFile != null)
			{
				this.StatFiles.Add(sectionFile);
			}
		}

		/// <summary>
		/// Determines the stat variables for meta data.
		/// </summary>
		/// <param name="dataIndex">The meta data index.</param>
		/// <returns></returns>
		private IEnumerable<StatVariable> DetermineStatVariablesForMetaData(string dataIndex)
		{
			var statVariables = new List<StatVariable>();

			if (!this.visibleColumnLookup.ContainsKey(dataIndex))
			{
				return statVariables;
			}

			switch (dataIndex)
			{
				case ColumnModel.SUBMISSION_ID:
					statVariables.Add(new StatVariable
					{
						Name = MetaVariableName.SUBMISSION_ID,
						Header = SanitiseString(ColumnHeaderHelper.GetHeader(this.columnHeaderLookup, ColumnModel.SUBMISSION_ID, MetaVariableName.SUBMISSION_ID)),
						Type = StatVariableType.Text,
						Width = 36
					});
					break;
				case ColumnModel.FIELDWORKER_NAME:
					statVariables.Add(new StatVariable
					{
						Name = MetaVariableName.FIELDWORKER_NAME,
						Header = SanitiseString(ColumnHeaderHelper.GetHeader(this.columnHeaderLookup, ColumnModel.FIELDWORKER_NAME, MetaVariableName.FIELDWORKER_NAME)),
						Type = StatVariableType.Text
					});
					statVariables.Add(new StatVariable
					{
						Name = MetaVariableName.FIELDWORKER_ID,
						Header = MetaVariableName.FIELDWORKER_ID,
						Type = StatVariableType.Text,
						Width = 36
					});
					break;
				case ColumnModel.DEVICE_NAME:
					statVariables.Add(new StatVariable
					{
						Name = MetaVariableName.DEVICE,
						Header = SanitiseString(ColumnHeaderHelper.GetHeader(this.columnHeaderLookup, ColumnModel.DEVICE_NAME, MetaVariableName.DEVICE)),
						Type = StatVariableType.Text
					});
					break;
				case ColumnModel.RECEIVED_DATE:
					statVariables.Add(new StatVariable
					{
						Name = MetaVariableName.RECEIVED,
						Header = SanitiseString(ColumnHeaderHelper.GetHeader(this.columnHeaderLookup, ColumnModel.RECEIVED_DATE, MetaVariableName.RECEIVED)),
						Type = StatVariableType.LongDate
					});
					break;
				case ColumnModel.START_DATE:
					statVariables.Add(new StatVariable
					{
						Name = MetaVariableName.START,
						Header = SanitiseString(ColumnHeaderHelper.GetHeader(this.columnHeaderLookup, ColumnModel.START_DATE, MetaVariableName.START)),
						Type = StatVariableType.LongDate
					});
					break;
				case ColumnModel.END_DATE:
					statVariables.Add(new StatVariable
					{
						Name = MetaVariableName.END,
						Header = SanitiseString(ColumnHeaderHelper.GetHeader(this.columnHeaderLookup, ColumnModel.END_DATE, MetaVariableName.END)),
						Type = StatVariableType.LongDate
					});
					break;
				case ColumnModel.DURATION_IN_SECONDS:
					statVariables.Add(new StatVariable
					{
						Name = MetaVariableName.DURATION_SECONDS,
						Header = SanitiseString(ColumnHeaderHelper.GetHeader(this.columnHeaderLookup, ColumnModel.DURATION_IN_SECONDS, MetaVariableName.DURATION_SECONDS)),
						Type = StatVariableType.Numeric
					});
					break;
				case ColumnModel.LATITUDE:
					statVariables.Add(new StatVariable
					{
						Name = MetaVariableName.LATITUDE,
						Header = SanitiseString(ColumnHeaderHelper.GetHeader(this.columnHeaderLookup, ColumnModel.LATITUDE, MetaVariableName.LATITUDE)),
						Type = StatVariableType.Numeric
					});
					break;
				case ColumnModel.LONGITUDE:
					statVariables.Add(new StatVariable
					{
						Name = MetaVariableName.LONGITUDE,
						Header = SanitiseString(ColumnHeaderHelper.GetHeader(this.columnHeaderLookup, ColumnModel.LONGITUDE, MetaVariableName.LONGITUDE)),
						Type = StatVariableType.Numeric
					});
					break;
				case ColumnModel.LANGUAGE:
					statVariables.Add(new StatVariable
					{
						Name = MetaVariableName.LANGUAGE,
						Header = SanitiseString(ColumnHeaderHelper.GetHeader(this.columnHeaderLookup, ColumnModel.LANGUAGE, MetaVariableName.LANGUAGE)),
						Type = StatVariableType.Text
					});
					break;
				case ColumnModel.SURVEY_VERSION:
					statVariables.Add(new StatVariable
					{
						Name = MetaVariableName.SURVEY_VERSION,
						Header = SanitiseString(ColumnHeaderHelper.GetHeader(this.columnHeaderLookup, ColumnModel.SURVEY_VERSION, MetaVariableName.SURVEY_VERSION)),
						Type = StatVariableType.Numeric
					});
					break;
				case ColumnModel.MODIFIED_BY:
					statVariables.Add(new StatVariable
					{
						Name = MetaVariableName.MODIFIED_BY,
						Header = SanitiseString(ColumnHeaderHelper.GetHeader(this.columnHeaderLookup, ColumnModel.MODIFIED_BY, MetaVariableName.MODIFIED_BY)),
						Type = StatVariableType.Text
					});
					break;
				case ColumnModel.MODIFIED_ON:
					statVariables.Add(new StatVariable
					{
						Name = MetaVariableName.MODIFIED_ON,
						Header = SanitiseString(ColumnHeaderHelper.GetHeader(this.columnHeaderLookup, ColumnModel.MODIFIED_ON, MetaVariableName.MODIFIED_ON)),
						Type = StatVariableType.LongDate
					});
					break;
				case ColumnModel.SECTION_INDEX:
					statVariables.Add(new StatVariable
					{
						Name = MetaVariableName.REPEATS_ON_QUESTION,
						Header = MetaVariableName.REPEATS_ON_QUESTION,
						Type = StatVariableType.Text
					});
					statVariables.Add(new StatVariable
					{
						Name = MetaVariableName.REPEAT_QUESTION_VALUE,
						Header = MetaVariableName.REPEAT_QUESTION_VALUE,
						Type = StatVariableType.Numeric
					});
					statVariables.Add(new StatVariable
					{
						Name = MetaVariableName.REPEATING_INDEX,
						Header = SanitiseString(ColumnHeaderHelper.GetHeader(this.columnHeaderLookup, ColumnModel.SECTION_INDEX, MetaVariableName.REPEATING_INDEX)),
						Type = StatVariableType.Numeric
					});
					break;
				case ColumnModel.IS_COMPLETE:
					statVariables.Add(new StatVariable
					{
						Name = MetaVariableName.COMPLETE,
						Header = SanitiseString(ColumnHeaderHelper.GetHeader(this.columnHeaderLookup, ColumnModel.IS_COMPLETE, MetaVariableName.COMPLETE)),
						Type = StatVariableType.Text
					});
					break;
			}
			return statVariables;
		}

		/// <summary>
		/// Determines the stat variables for a question.
		/// </summary>
		/// <param name="question">The question.</param>
		/// <param name="validQuestionName">Valid name of the question.</param>
		/// <param name="instance">The instance.</param>
		/// <returns></returns>
		private IEnumerable<StatVariable> DetermineStatVariablesForQuestion(QuestionEntity question, string validQuestionName, int? instance = null)
		{
			var statVariables = new List<StatVariable>();
			var header = SanitiseString(ColumnHeaderHelper.GetHeader(this.columnHeaderLookup, question, validQuestionName));

			switch (question.Type)
			{
				case QuestionType.Binary:
				case QuestionType.Canvas_Windows_Mobile_only:
				case QuestionType.Photo_capture:
				case QuestionType.Instruction:
					break;
				case QuestionType.Email_address:
				case QuestionType.Free_Text:
				case QuestionType.Multiline:
				case QuestionType.Password:
				case QuestionType.Phone_number:
				case QuestionType.Numeric_identifier:
				case QuestionType.PIN:
				case QuestionType.Predictive:
				case QuestionType.Rank:
				case QuestionType.Single_line:
				case QuestionType.Unknown:
					statVariables.Add(new StatVariable
										{
											Question = question,
											Instance = instance,
											Name = instance.HasValue ? "{0}_{1}".FormatText(validQuestionName, instance) : validQuestionName,
											Header = instance.HasValue ? "{0}_{1}".FormatText(header, instance) : header,
											Label = question.Label,
											Type = StatVariableType.Text
										});
					break;
				case QuestionType.Single:
					statVariables.Add(new StatVariable
										{
											Question = question,
											Instance = instance,
											Name = instance.HasValue ? "{0}_{1}".FormatText(validQuestionName, instance) : validQuestionName,
											Header = instance.HasValue ? "{0}_{1}".FormatText(header, instance) : header,
											Label = question.Label,
											OptionTexts = this.isFormatStatTransfer || this.useSingleValues ? null : question.Options.ToDictionary(option => option.Value, option => option.Text),
											Type = StatVariableType.Text,
											IsAllNumeric = true
										});
					break;
				case QuestionType.Date:
					statVariables.Add(new StatVariable
										{
											Question = question,
											Instance = instance,
											Name = instance.HasValue ? "{0}_{1}".FormatText(validQuestionName, instance) : validQuestionName,
											Header = instance.HasValue ? "{0}_{1}".FormatText(header, instance) : header,
											Label = question.Label,
											Type = StatVariableType.ShortDate
										});
					break;
				case QuestionType.Decimal:
				case QuestionType.Integer:
				case QuestionType.GS1_identifier:
					statVariables.Add(new StatVariable
										{
											Question = question,
											Instance = instance,
											Name = instance.HasValue ? "{0}_{1}".FormatText(validQuestionName, instance) : validQuestionName,
											Header = instance.HasValue ? "{0}_{1}".FormatText(header, instance) : header,
											Label = question.Label,
											Type = StatVariableType.Numeric
										});
					break;
				case QuestionType.Multiple:
					foreach (var option in question.Options)
					{
						string optionSuffix = SanitiseString(this.shortNamingOption ? option.Value : option.Text);
						string label = header.RegexReplace("}|{", "_");

						statVariables.Add(new StatVariable
											{
												Question = question,
												Instance = instance,
												Name = instance.HasValue ? "{0}_{1}_{2}".FormatText(validQuestionName, instance, optionSuffix) : "{0}_{1}".FormatText(validQuestionName, optionSuffix),
												Header = instance.HasValue ? "{0}_{1}_{2}".FormatText(header, instance, optionSuffix) : "{0}_{1}".FormatText(header, optionSuffix),
												Label = question.Label,
												QuestionLabel = instance.HasValue ? "{0}_{1}".FormatText(label, instance) : label,
												OptionValue = option.Value.Trim(),
												OptionText = option.Text,
												Type = StatVariableType.Numeric
											});
					}
					break;
				case QuestionType.Time:
					statVariables.Add(new StatVariable
										{
											Question = question,
											Instance = instance,
											Name = instance.HasValue ? "{0}_{1}".FormatText(validQuestionName, instance) : validQuestionName,
											Header = instance.HasValue ? "{0}_{1}".FormatText(header, instance) : header,
											Label = question.Label,
											Type = StatVariableType.Time
										});
					break;
				case QuestionType.Variable:
					var variableType = question.Label.ToEnum<VariableType>();
					if (variableType == VariableType.Binary || variableType == VariableType.Operator)
					{
						break;
					}
					if (variableType == VariableType.Numeric || variableType == VariableType.Decimal)
					{
						statVariables.Add(new StatVariable
											{
												Question = question,
												Instance = instance,
												Name = instance.HasValue ? "{0}_{1}".FormatText(validQuestionName, instance) : validQuestionName,
												Header = instance.HasValue ? "{0}_{1}".FormatText(header, instance) : header,
												Label = question.Label,
												Type = StatVariableType.Numeric
											});
					}
					else
					{
						statVariables.Add(new StatVariable
											{
												Question = question,
												Instance = instance,
												Name = instance.HasValue ? "{0}_{1}".FormatText(validQuestionName, instance) : validQuestionName,
												Header = instance.HasValue ? "{0}_{1}".FormatText(header, instance) : header,
												Label = question.Label,
												Type = StatVariableType.Text
											});
					}
					break;
				case QuestionType.GPS:
					statVariables.Add(new StatVariable
					{
						Question = question,
						Instance = instance,
						Name = instance.HasValue ? "{0}_{1}".FormatText(validQuestionName, instance) : validQuestionName,
						Header = instance.HasValue ? "{0}_{1}".FormatText(header, instance) : header,
						Label = question.Label,
						Type = StatVariableType.Text,
						GPSColumnType = GpsColumnType.General
					});
					var statVariableName = "{0}{1}".FormatText(validQuestionName, GpsColumnType.Latitude);
					var statVariableHeader = "{0}{1}".FormatText(header, GpsColumnType.Latitude.GetSuffix());
					statVariables.Add(new StatVariable
					{
						Question = question,
						Instance = instance,
						Name = instance.HasValue ? "{0}_{1}".FormatText(statVariableName, instance) : statVariableName,
						Header = instance.HasValue ? "{0}_{1}".FormatText(statVariableHeader, instance) : statVariableHeader,
						Label = question.Label,
						Type = StatVariableType.Numeric,
						GPSColumnType = GpsColumnType.Latitude
					});
					statVariableName = "{0}{1}".FormatText(validQuestionName, GpsColumnType.Longitude);
					statVariableHeader = "{0}{1}".FormatText(header, GpsColumnType.Longitude.GetSuffix());
					statVariables.Add(new StatVariable
					{
						Question = question,
						Instance = instance,
						Name = instance.HasValue ? "{0}_{1}".FormatText(statVariableName, instance) : statVariableName,
						Header = instance.HasValue ? "{0}_{1}".FormatText(statVariableHeader, instance) : statVariableHeader,
						Label = question.Label,
						Type = StatVariableType.Numeric,
						GPSColumnType = GpsColumnType.Longitude
					});
					statVariableName = "{0}{1}".FormatText(validQuestionName, GpsColumnType.Altitude);
					statVariableHeader = "{0}{1}".FormatText(header, GpsColumnType.Altitude.GetSuffix());
					statVariables.Add(new StatVariable
					{
						Question = question,
						Instance = instance,
						Name = instance.HasValue ? "{0}_{1}".FormatText(statVariableName, instance) : statVariableName,
						Header = instance.HasValue ? "{0}_{1}".FormatText(statVariableHeader, instance) : statVariableHeader,
						Label = question.Label,
						Type = StatVariableType.Numeric,
						GPSColumnType = GpsColumnType.Altitude
					});
					statVariableName = "{0}{1}".FormatText(validQuestionName, GpsColumnType.Time);
					statVariableHeader = "{0}{1}".FormatText(header, GpsColumnType.Time.GetSuffix());
					statVariables.Add(new StatVariable
					{
						Question = question,
						Instance = instance,
						Name = instance.HasValue ? "{0}_{1}".FormatText(statVariableName, instance) : statVariableName,
						Header = instance.HasValue ? "{0}_{1}".FormatText(statVariableHeader, instance) : statVariableHeader,
						Label = question.Label,
						Type = StatVariableType.LongDate,
						GPSColumnType = GpsColumnType.Time
					});
					break;
				default:
					statVariables.Add(new StatVariable
										{
											Question = question,
											Instance = instance,
											Name = instance.HasValue ? "{0}_{1}".FormatText(validQuestionName, instance) : validQuestionName,
											Header = instance.HasValue ? "{0}_{1}".FormatText(header, instance) : header,
											Label = question.Label,
											Type = StatVariableType.Text
										});
					break;
			}
			return statVariables;
		}

		/// <summary>
		/// Write the headings to the data file
		/// </summary>
		private void WriteDataHeadings()
		{
			foreach (var statFile in this.StatFiles)
			{
				statFile.Writer.WriteLine(string.Join(",", statFile.StatVariables.Select(v => "\"{0}\"".FormatText(v.Header))));
				statFile.Writer.Flush();
			}
		}

		/// <summary>
		/// Write the meta data to the data file
		/// </summary>
		private void WriteMetaData(StreamWriter streamWriter, Models.Submission submission, StatVariable statVariable, int? instance = null, int? repeatingQuestionId = null)
		{
			bool enclose = this.enforceEncapsulation;
			if (!enclose)
			{
				switch (statVariable.Name)
				{
					// If encapsulation is not enforced (i.e. this is not a CSV export), string data still needs to be enclosed
					case MetaVariableName.SUBMISSION_ID:
					case MetaVariableName.FIELDWORKER_NAME:
					case MetaVariableName.FIELDWORKER_ID:
					case MetaVariableName.DEVICE:
					case MetaVariableName.LANGUAGE:
					case MetaVariableName.MODIFIED_BY:
					case MetaVariableName.REPEATS_ON_QUESTION:
					case MetaVariableName.COMPLETE:
						enclose = true;
						break;
				}
			}

			if (enclose)
			{
				streamWriter.Write("\"");
			}

			switch (statVariable.Name)
			{
				case MetaVariableName.SUBMISSION_ID:
					streamWriter.Write(submission.Id);
					break;
				case MetaVariableName.FIELDWORKER_NAME:
					var responseValue = SanitiseString(submission.FieldworkerName);
					streamWriter.Write(responseValue);
					if (responseValue.Length > statVariable.Width)
					{
						statVariable.Width = responseValue.Length;
					}
					break;
				case MetaVariableName.FIELDWORKER_ID:
					streamWriter.Write(submission.FieldworkerId);
					break;
				case MetaVariableName.DEVICE:
					responseValue = SanitiseString(DeviceEntity.FormatHandsetInfo(submission.DeviceModelDescription, submission.DeviceAssetCode));
					streamWriter.Write(responseValue);
					if (responseValue.Length > statVariable.Width)
					{
						statVariable.Width = responseValue.Length;
					}
					break;
				case MetaVariableName.RECEIVED:
					streamWriter.Write(submission.RecievedDate.ToString("d-M-yyyy HH:mm:ss"));
					break;
				case MetaVariableName.START:
					streamWriter.Write(submission.StartDate.ToString("d-M-yyyy HH:mm:ss"));
					break;
				case MetaVariableName.END:
					streamWriter.Write(submission.EndDate.ToString("d-M-yyyy HH:mm:ss"));
					break;
				case MetaVariableName.DURATION_SECONDS:
					streamWriter.Write(submission.DurationInSeconds);
					break;
				case MetaVariableName.LATITUDE:
					if (submission.LocationAvailable)
					{
						streamWriter.Write(submission.Latitude);
					}
					break;
				case MetaVariableName.LONGITUDE:
					if (submission.LocationAvailable)
					{
						streamWriter.Write(submission.Longitude);
					}
					break;
				case MetaVariableName.LANGUAGE:
					responseValue = SanitiseString(submission.Language);
					streamWriter.Write(responseValue);
					if (responseValue.Length > statVariable.Width)
					{
						statVariable.Width = responseValue.Length;
					}
					break;
				case MetaVariableName.SURVEY_VERSION:
					streamWriter.Write(submission.SurveyVersion);
					break;
				case MetaVariableName.MODIFIED_BY:
					responseValue = SanitiseString(submission.LastModifiedBy);
					streamWriter.Write(responseValue);
					if (responseValue.Length > statVariable.Width)
					{
						statVariable.Width = responseValue.Length;
					}
					break;
				case MetaVariableName.MODIFIED_ON:
					streamWriter.Write(submission.LastModifiedOn.ToString("d-M-yyyy HH:mm:ss"));
					break;
				case MetaVariableName.REPEATS_ON_QUESTION:
					if (repeatingQuestionId.HasValue)
					{
						QuestionEntity repeatingQuestion = this.questionLookup[repeatingQuestionId.Value];
						if (repeatingQuestion != null)
						{
							var repeatingQuestionName = SanitiseString(repeatingQuestion.name);
							streamWriter.Write(repeatingQuestionName);
							if (repeatingQuestionName.Length > statVariable.Width)
							{
								statVariable.Width = repeatingQuestionName.Length;
							}
						}
					}
					break;
				case MetaVariableName.REPEAT_QUESTION_VALUE:
					if (repeatingQuestionId.HasValue)
					{
						FieldResponseSummary fieldResponse;
						if (submission.FieldResponseLookup.TryGetValue(repeatingQuestionId.Value, out fieldResponse))
						{
							var repeatingQuestionResponse = fieldResponse.Responses.FirstOrDefault();
							if (repeatingQuestionResponse != null)
							{
								streamWriter.Write(repeatingQuestionResponse.Value);
							}
						}
					}
					break;
				case MetaVariableName.REPEATING_INDEX:
					streamWriter.Write(instance);
					break;
				case MetaVariableName.COMPLETE:
					responseValue = submission.IsComplete ? "Yes" : "No";
					streamWriter.Write(responseValue);
					if (responseValue.Length > statVariable.Width)
					{
						statVariable.Width = responseValue.Length;
					}
					break;
			}

			if (enclose)
			{
				streamWriter.Write("\"");
			}
		}

		/// <summary>
		/// Write the question data to the data file
		/// </summary>
		private void WriteQuestionData(StreamWriter streamWriter, IEnumerable<Models.Response> responses, StatVariable statVariable, int? instance = null)
		{
			var response = responses.FirstOrDefault(r => !instance.HasValue || r.Instance == instance.Value);
			if (response == null || response.Value.IsNullOrEmpty())
			{
				return;
			}

			bool enclose = this.enforceEncapsulation;
			if (!enclose && statVariable.Type == StatVariableType.Text)
			{
				// If encapsulation is not enforced (i.e. this is not a CSV export), string data still needs to be enclosed
				enclose = true;
			}

			if (enclose)
			{
				streamWriter.Write("\"");
			}

			if (statVariable.Question.Type == QuestionType.Multiple)
			{
				IEnumerable<string> values = response.Value.Split("||").Select(x => x.Trim());
				streamWriter.Write(values.Contains(statVariable.OptionValue) ? "1" : "0");
			}
			else if (statVariable.Question.Type == QuestionType.Single)
			{
				string responseValue = this.isFormatStatTransfer || this.useSingleValues || !statVariable.OptionTexts.ContainsKey(response.Value) ? SanitiseString(response.Value) : SanitiseString(statVariable.OptionTexts[response.Value]);
				streamWriter.Write(responseValue);
				if (responseValue.Length > statVariable.Width)
				{
					statVariable.Width = responseValue.Length;
				}
				if (!Regex.IsMatch(responseValue, @"^\d+$"))
				{
					statVariable.IsAllNumeric = false;
				}
			}
			else if (statVariable.Question.Type == QuestionType.GPS)
			{
				NMEAWrapper wrapper = new NMEAWrapper(response.Value);
				switch (statVariable.GPSColumnType)
				{
					case GpsColumnType.General:
						string responseValue = wrapper.ToString(NMEAWrapper.StringFormat.LatitudeLongitudeTimestamp);
						streamWriter.Write(responseValue);
						if (responseValue.Length > statVariable.Width)
						{
							statVariable.Width = responseValue.Length;
						}
						break;
					case GpsColumnType.Latitude:
						streamWriter.Write(wrapper.GPSPoint.Latitude.ToString());
						break;
					case GpsColumnType.Longitude:
						streamWriter.Write(wrapper.GPSPoint.Longitude.ToString());
						break;
					case GpsColumnType.Altitude:
						streamWriter.Write(wrapper.GPSPoint.Altitude.ToString());
						break;
					case GpsColumnType.Time:
						streamWriter.Write(wrapper.GPSPoint.Timestamp.HasValue ? wrapper.GPSPoint.Timestamp.Value.ToString("d-M-yyyy HH:mm:ss") : null);
						break;
				}
			}
			else
			{
				switch (statVariable.Type)
				{
					case StatVariableType.Numeric:
					case StatVariableType.LongDate:
					case StatVariableType.ShortDate:
					case StatVariableType.Time:
						streamWriter.Write(response.Value);
						break;
					default:
						string responseValue = SanitiseString(response.Value);
						streamWriter.Write(responseValue);
						if (responseValue.Length > statVariable.Width)
						{
							statVariable.Width = responseValue.Length;
						}
						break;
				}
			}

			if (enclose)
			{
				streamWriter.Write("\"");
			}
		}

		/// <summary>
		/// Writes the schema file.
		/// </summary>
		private void WriteSchema(StreamWriter streamWriter, StatFile statFile)
		{
			streamWriter.WriteLine("ENCODING UTF-8");
			streamWriter.WriteLine();
			streamWriter.WriteLine("FORMAT delimited commas");
			streamWriter.WriteLine();
			streamWriter.WriteLine("FIRST LINE 2");
			streamWriter.WriteLine();
			streamWriter.WriteLine("VARIABLES");

			// Determine the value labels for select-list questions
			string valueLabelFormatPrefix = "VL_{0}_{1}".FormatText(this.analyticsFilter.SurveyId, statFile.SectionId.HasValue ? statFile.SectionId.Value.ToString() : statFile.Name);

			int valueLabelIndex = 0;
			var statValueLabels = new List<StatValueLabel>();
			foreach (var variable in statFile.StatVariables.Where(v => v.Question != null && (v.Question.Type == QuestionType.Single || v.Question.Type == QuestionType.Multiple)))
			{
				var statValueLabel = new StatValueLabel
				{
					Tag = "{0}_{1}".FormatText(valueLabelFormatPrefix, valueLabelIndex++),
					ValueLabelPairs = new List<StatValueLabelPair>()
				};

				if (variable.Question.Type == QuestionType.Multiple)
				{
					statValueLabel.ValueLabelPairs.Add(new StatValueLabelPair
					{
						Value = "1",
						Label = "Y - {0}".FormatText(variable.OptionText)
					});
					statValueLabel.ValueLabelPairs.Add(new StatValueLabelPair
					{
						Value = "0",
						Label = "N - {0}".FormatText(variable.OptionText)
					});
				}
				else if (variable.Question.Type == QuestionType.Single && variable.IsAllNumeric)
				{
					foreach (var option in variable.Question.Options)
					{
						if (Regex.IsMatch(option.Value, @"^\d+$"))
						{
							statValueLabel.ValueLabelPairs.Add(new StatValueLabelPair
							{
								Value = option.Value,
								Label = option.Text
							});
						}
					}
				}

				var existingStatValueLabel = statValueLabels.FirstOrDefault(x => x.IsEquivalent(statValueLabel));
				if (existingStatValueLabel == null)
				{
					variable.ValueLabel = statValueLabel;
					statValueLabels.Add(statValueLabel);
				}
				else
				{
					valueLabelIndex--;
					variable.ValueLabel = existingStatValueLabel;
				}
			}

			// Write the stat variables
			foreach (var variable in statFile.StatVariables)
			{
				if (variable.Question != null && variable.Question.Type == QuestionType.Multiple)
				{
					streamWriter.WriteLine("\t\"{0}\"\t({1})\t{{{2}}}\t\\{3}", variable.Header, DetermineVariableTypeString(variable), variable.Label.RegexReplace(@"[\r\n}]", ""), variable.ValueLabel.Tag);
				}
				else if (variable.Question != null && variable.Question.Type == QuestionType.Single && variable.IsAllNumeric)
				{
					streamWriter.WriteLine("\t\"{0}\"\t({1})\t{{{2}}}\t\\{3}", variable.Header, DetermineVariableTypeString(variable), variable.Label.RegexReplace(@"[\r\n}]", ""), variable.ValueLabel.Tag);
				}
				else if (variable.Question != null)
				{
					streamWriter.WriteLine("\t\"{0}\"\t({1})\t{{{2}}}", variable.Header, DetermineVariableTypeString(variable), variable.Label.RegexReplace(@"[\r\n}]", ""));
				}
				else
				{
					streamWriter.WriteLine("\t\"{0}\"\t({1})", variable.Header, DetermineVariableTypeString(variable));
				}
			}

			// Write the value labels for select-list questions
			if (statValueLabels.Count > 0)
			{
				streamWriter.WriteLine();
				streamWriter.WriteLine("VALUE LABELS");
				foreach (var statValueLabel in statValueLabels)
				{
					streamWriter.WriteLine("\t\\{0}", statValueLabel.Tag);
					foreach (var valueLabelPair in statValueLabel.ValueLabelPairs)
					{
						streamWriter.WriteLine("\t\t{0}\t\"{1}\"", valueLabelPair.Value, valueLabelPair.Label);
					}
				}
			}
		}

		/// <summary>
		/// Writes the code book file.
		/// </summary>
		/// <param name="streamWriter">The stream writer.</param>
		private void WriteCodebook(StreamWriter streamWriter)
		{
			streamWriter.WriteLine("\"Question\",\"Variable\",\"Value\",\"Label\"");
			foreach (var variable in this.StatFiles.SelectMany(x => x.StatVariables).Where(v => v.Question != null && (v.Question.Type == QuestionType.Single || v.Question.Type == QuestionType.Multiple)))
			{
				if (variable.Question.Type == QuestionType.Multiple)
				{
					streamWriter.WriteLine("\"{0}\",\"{1}\",\"1\",\"Y - {2}\"", variable.QuestionLabel, variable.Header, variable.OptionText);
					streamWriter.WriteLine("\"{0}\",\"{1}\",\"0\",\"N - {2}\"", variable.QuestionLabel, variable.Header, variable.OptionText);
				}
				else if (variable.Question.Type == QuestionType.Single)
				{
					foreach (var option in variable.Question.Options)
					{
						streamWriter.WriteLine("\"{0}\",\"{0}\",\"{1}\",\"{2}\"", variable.Header, option.Value, option.Text);
					}
				}
			}
		}

		/// <summary>
		/// Writes the questions file.
		/// </summary>
		/// <param name="streamWriter">The stream writer.</param>
		private void WriteQuestions(StreamWriter streamWriter)
		{
			int rowIndex = 1;
			streamWriter.WriteLine("\"Question name\",\"#\",\"Section\",\"Question Id\",\"Question text\",\"Question type\"");
			foreach (var question in this.questionLookup)
			{
				if (!this.visibleColumnLookup.ContainsKey("Question_{0}".FormatText(question.Value.id)) || !question.Value.section_id.HasValue)
				{
					continue;
				}

				var sanitisedQuestionName = SanitiseString(question.Value.name);
				sanitisedQuestionName = SanitiseString(ColumnHeaderHelper.GetHeader(this.columnHeaderLookup, question.Value, sanitisedQuestionName));
				streamWriter.WriteLine("\"{0}\",\"{1}\",\"{2}\",\"{3}\",\"{4}\",\"{5}\"", sanitisedQuestionName, rowIndex++, question.Value.SurveySection.title, question.Value.id, question.Value.Label, question.Value.QuestionTypeEntity.name);
			}
		}

		/// <summary>
		/// Sanitises the string (removes line breaks).
		/// </summary>
		/// <param name="value">The value.</param>
		/// <returns></returns>
		private static string SanitiseString(string value)
		{
			return value.RegexReplace("\r|\n|\r\n", " ").RegexReplace("\"", "\"\"");
		}

		/// <summary>
		/// Determines the variable type string.
		/// </summary>
		/// <param name="variable">The variable.</param>
		/// <returns></returns>
		private static string DetermineVariableTypeString(StatVariable variable)
		{
			if (variable.Question != null && variable.Question.Type == QuestionType.Single && variable.IsAllNumeric)
			{
				return "F";
			}

			switch (variable.Type)
			{
				case StatVariableType.Text:
					return "A{0}".FormatText(variable.Width);
				case StatVariableType.Numeric:
					return "F";
				case StatVariableType.LongDate:
					return "%d-%m-%Y %H:%M:%S";
				case StatVariableType.ShortDate:
					return "%d-%m-%Y";
				case StatVariableType.Time:
					return "%H:%M";
				default:
					return "A";
			}
		}

		private class StatFile
		{
			public string Name;
			public string FilePath;
			public StreamWriter Writer;
			public List<StatVariable> StatVariables;
			public bool FirstStatVariable;
			public int? SectionId;
			public bool IsRepeating;
			public int? RepeatingQuestionId;
		}

		private class StructuralFile
		{
			public string FilePath;
			public StreamWriter FileWriter;
		}

		private class StatVariable
		{
			public QuestionEntity Question;
			public int? Instance;
			public string Name;
			public string Header;
			public string OptionValue;
			public string OptionText;
			public Dictionary<string, string> OptionTexts;
			public StatVariableType Type;
			public int Width;
			public string Label;
			public string QuestionLabel;
			public bool IsAllNumeric;
			public StatValueLabel ValueLabel;
			public GpsColumnType GPSColumnType;
		}

		private enum StatVariableType
		{
			Text,
			Numeric,
			LongDate,
			ShortDate,
			Time
		}

		private class StatSection
		{
			public int Id;
			public bool IsRepeating;
		}

		private class StatValueLabel
		{
			public string Tag;
			public List<StatValueLabelPair> ValueLabelPairs;

			public bool IsEquivalent(StatValueLabel other)
			{
				if (this.ValueLabelPairs.Count != other.ValueLabelPairs.Count)
				{
					return false;
				}

				foreach (var valueLabelPair in this.ValueLabelPairs)
				{
					if (!other.ValueLabelPairs.Contains(valueLabelPair))
					{
						return false;
					}
				}

				foreach (var valueLabelPair in other.ValueLabelPairs)
				{
					if (!this.ValueLabelPairs.Contains(valueLabelPair))
					{
						return false;
					}
				}

				return true;
			}
		}

		private class StatValueLabelPair
		{
			public string Value;
			public string Label;

			public override bool Equals(object obj)
			{
				var item = obj as StatValueLabelPair;

				if (item == null)
				{
					return false;
				}

				return (this.Value == item.Value) && (this.Label == item.Label);
			}
		}

		private static class MetaVariableName
		{
			public const string SUBMISSION_ID = "Submission Id";
			public const string FIELDWORKER_NAME = "Fieldworker Name";
			public const string FIELDWORKER_ID = "Fieldworker Id";
			public const string DEVICE = "Device";
			public const string RECEIVED = "Received";
			public const string START = "Start";
			public const string END = "End";
			public const string DURATION_SECONDS = "Duration (seconds)";
			public const string LATITUDE = "Latitude";
			public const string LONGITUDE = "Longitude";
			public const string LANGUAGE = "Language";
			public const string SURVEY_VERSION = "Survey Version";
			public const string MODIFIED_BY = "Modified By";
			public const string MODIFIED_ON = "Modified On";
			public const string REPEATS_ON_QUESTION = "Repeats On Question";
			public const string REPEAT_QUESTION_VALUE = "Repeat Question Value";
			public const string REPEATING_INDEX = "Repeating Index";
			public const string COMPLETE = "Complete";
		}
	}
}