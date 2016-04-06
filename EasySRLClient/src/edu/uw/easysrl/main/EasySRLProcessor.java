package edu.uw.easysrl.main;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;
import java.util.InputMismatchException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import uk.co.flamingpenguin.jewel.cli.ArgumentValidationException;
import uk.co.flamingpenguin.jewel.cli.CliFactory;
import uk.co.flamingpenguin.jewel.cli.Option;

import com.google.common.base.Stopwatch;

import edu.uw.easysrl.dependencies.Coindexation;
import edu.uw.easysrl.semantics.lexicon.CompositeLexicon;
import edu.uw.easysrl.semantics.lexicon.Lexicon;
import edu.uw.easysrl.syntax.evaluation.CCGBankEvaluation;
import edu.uw.easysrl.syntax.grammar.Category;
import edu.uw.easysrl.syntax.model.CutoffsDictionaryInterface;
import edu.uw.easysrl.syntax.model.Model.ModelFactory;
import edu.uw.easysrl.syntax.model.SRLFactoredModel.SRLFactoredModelFactory;
import edu.uw.easysrl.syntax.model.SupertagFactoredModel.SupertagFactoredModelFactory;
import edu.uw.easysrl.syntax.model.feature.Feature.FeatureKey;
import edu.uw.easysrl.syntax.model.feature.FeatureSet;
import edu.uw.easysrl.syntax.parser.Parser;
import edu.uw.easysrl.syntax.parser.ParserAStar;
import edu.uw.easysrl.syntax.parser.ParserBeamSearch;
import edu.uw.easysrl.syntax.parser.ParserCKY;
import edu.uw.easysrl.syntax.parser.SRLParser;
import edu.uw.easysrl.syntax.parser.SRLParser.BackoffSRLParser;
import edu.uw.easysrl.syntax.parser.SRLParser.CCGandSRLparse;
import edu.uw.easysrl.syntax.parser.SRLParser.JointSRLParser;
import edu.uw.easysrl.syntax.parser.SRLParser.PipelineSRLParser;
import edu.uw.easysrl.syntax.parser.SRLParser.SemanticParser;
import edu.uw.easysrl.syntax.tagger.POSTagger;
import edu.uw.easysrl.syntax.tagger.Tagger;
import edu.uw.easysrl.syntax.tagger.TaggerEmbeddings;
import edu.uw.easysrl.syntax.training.PipelineTrainer.LabelClassifier;
import edu.uw.easysrl.syntax.training.Training;
import edu.uw.easysrl.util.Util;
import edu.uw.easysrl.main.EasySRL;
import edu.uw.easysrl.main.InputReader;
import edu.uw.easysrl.main.ParsePrinter;
import edu.uw.easysrl.main.EasySRL.CommandLineArguments;

public class EasySRLProcessor {

  private int id = 0;
  private SRLParser srlParser;
  private final ParsePrinter srlPrinter = EasySRL.OutputFormat.SRL.printer;
	private final InputReader srlReader = InputReader.make(EasySRL.InputFormat.TOKENIZED);

  public EasySRLProcessor(String srlModelPath) {
    try {
      if (srlModelPath == null) {
        System.err.println("Set NOUS_SRL_MODEL config variable");
        System.exit(1);
      }
      String[] args = {};
			final CommandLineArguments commandLineOptions = 
          CliFactory.parseArguments(CommandLineArguments.class, args);
			final EasySRL.InputFormat input = EasySRL.InputFormat.POSANDNERTAGGED;
			final File modelFolder = Util.getFile(srlModelPath);

			if (!modelFolder.exists()) {
				System.err.println("Couldn't load model from: " + modelFolder);
        System.exit(1);
			}

			final File pipelineFolder = new File(modelFolder, "/pipeline");
			System.err.println("====Starting loading model====");

			if (pipelineFolder.exists()) {
				final POSTagger posTagger = 
            POSTagger.getStanfordTagger(new File(pipelineFolder, "posTagger"));
				final PipelineSRLParser pipeline = 
            EasySRL.makePipelineParser(pipelineFolder, commandLineOptions, 0.000001,
						    srlPrinter.outputsDependencies());
				srlParser = new BackoffSRLParser(
            new JointSRLParser(
                EasySRL.makeParser(commandLineOptions, 20000, true,
						        Optional.of(commandLineOptions.getSupertaggerWeight())), 
                posTagger), 
            pipeline);
			} else {
				srlParser = EasySRL.makePipelineParser(modelFolder, commandLineOptions, 0.000001, srlPrinter.outputsDependencies());
			}
    }
    catch(ArgumentValidationException ave) {
			System.err.println(ave.getMessage());
    }
    catch(IOException ioe) {
			System.err.println(ioe.getMessage());
    }
	}


	public String process(String line) throws IOException {
    final List<CCGandSRLparse> parses = srlParser.parseTokens(srlReader.readInput(line)
							.getInputWords());
		final String output = srlPrinter.printJointParses(parses, id++);
    return output;
	}


	public static void main(String[] cmdArgs) {
    try {
      String srlModelPath = "SET_ME_RIGHT";
      EasySRLProcessor processor = new EasySRLProcessor(srlModelPath);
      // EasySRLProcessor.run(cmdArgs);
			final Iterator<String> inputLines = Util.readFile(Util.getFile(cmdArgs[0])).iterator();
      String input = "Sutanay visited the PNNL office in Seattle";
      for (int i = 0; i < 1000000; i++) {
        String output = processor.process(input);
        if ((i % 10000) == 0) {
          System.out.println("Finished " + i + " iterations");
        }
      }
			/*while (inputLines.hasNext()) {
				// Read each sentence, either from STDIN or a parse.
				final String line = inputLines.next();
        String output = processor.process(line);
        //System.out.println(output);
      }*/
    } catch (IOException ioe) {
			System.err.println(ioe.getMessage());
    }
    // catch (InterruptedException ie) {
			// System.err.println(ie.getMessage());
    // }
  }
	
}
