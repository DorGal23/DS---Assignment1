package Stanford;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

public class NamedEntityRecognitionHandler {

	private final StanfordCoreNLP NERPipeline;
	private final ArrayList<String> acceptedEntities;

	public NamedEntityRecognitionHandler() {
		acceptedEntities = new ArrayList<>(3);
		acceptedEntities.add("PERSON");
		acceptedEntities.add("ORGANIZATION");
		acceptedEntities.add("LOCATION");

		Properties props = new Properties();
		props.put("annotators", "tokenize , ssplit, pos, lemma, ner");
		props.setProperty("tokenize.options", "untokenizable=noneDelete");
		NERPipeline = new StanfordCoreNLP(props);
	}

	public String findEntities(String review) {
		String entities = "";
		String newEntities = "";
		Annotation document = new Annotation(review);
		NERPipeline.annotate(document);
		List<CoreMap> sentences = document.get(SentencesAnnotation.class);
		for (CoreMap sentence : sentences) {
			for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
				String word = token.get(TextAnnotation.class);
				String keyWord = token.get(NamedEntityTagAnnotation.class);
				if (acceptedEntities.contains(keyWord))
					newEntities = entities+word+":"+keyWord+", ";
					entities = word+":"+keyWord+", ";
			}
		}
		return "["+newEntities+"]";
	}
}
