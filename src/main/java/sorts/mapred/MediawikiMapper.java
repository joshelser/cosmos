package sorts.mapred;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.mediawiki.xml.export_0.MediaWikiType;
import org.mediawiki.xml.export_0.PageType;

import com.sun.xml.bind.api.ClassResolver;

/**
 * 
 */
public class MediawikiMapper extends Mapper<LongWritable,Text,Text,Mutation> {

  private static Object lock = new Object();
  private static JAXBContext jaxbCtx;
  
  private Unmarshaller unmarshaller;

  public MediawikiMapper() {
    synchronized(lock) {
      try {
        jaxbCtx = JAXBContext.newInstance("org.mediawiki.xml.export_0", ClassLoader.getSystemClassLoader());
      } catch (JAXBException e) {
        throw new RuntimeException(e);
      }
    }
  }
  
  /**
   * Called once at the beginning of the task.
   */
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    try {
      this.unmarshaller = jaxbCtx.createUnmarshaller();
      this.unmarshaller.setProperty(ClassResolver.class.getName(), new ClassResolver() {

        @Override
        public Class<?> resolveElementName(String nsUri, String localName) throws Exception {
          if ("page".equalsIgnoreCase(localName)) {
            return PageType.class;
          } else {
            return MediaWikiType.class;
          }
        }
        
      });
    } catch (JAXBException e) {
      throw new RuntimeException(e);
    }
  }
  
  /**
   * Called once for each key/value pair in the input split. Most applications should override this, but the default is the identity function.
   */
  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    Object o;
    try {
      o = unmarshaller.unmarshal(new ByteArrayInputStream(value.getBytes(), 0, value.getLength()));
    } catch (JAXBException e) {
      throw new IOException("Couldn't unmarshall '" + value + "'", e);
    }
    
    
    PageType page = (PageType) o;
    System.out.println(key + ":" + page.getId());
    
  }
  
  /**
   * Called once at the end of the task.
   */
  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
  }
}
