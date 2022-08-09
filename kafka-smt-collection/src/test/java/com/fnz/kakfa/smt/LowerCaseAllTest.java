package com.fnz.kakfa.smt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fnz.kakfa.smt.LowerCaseAll.RenamedSchema;

public class LowerCaseAllTest {
    LowerCaseAll<SourceRecord> lca = new LowerCaseAll<SourceRecord>();
    
    final static Schema keySchema = keySchema();
    final static Schema valueSchema = valueSchema();

    @BeforeEach
    protected void setUp() throws Exception {
    }

//    @Test
//    public void testApplyWithSchema() {
////        SourceRecord{sourcePartition={server=tracey.servers.jhc.co.uk}, sourceOffset={offset.event_sequence=197082, offset.receiver_library=F63QULDVES, offset.receiver=FIGJRN0205, table.include.list=F63QULDVES.AKBAL,F63QULDVES.ASSET_RISK_SCORE,F63QULDVES.CGTPOOL,F63QULDVES.CGTSTAT,F63QULDVES.CGT_SUMMARY,F63QULDVES.CLIENT,F63QULDVES.CLIEXT,F63QULDVES.COMDES,F63QULDVES.CURRENCY,F63QULDVES.EXTADD,F63QULDVES.FINANCIAL_ACCOUNT,F63QULDVES.FMTSTRUCT,F63QULDVES.FXCODE,F63QULDVES.HOLD,F63QULDVES.INVESTMENT_OBJECTIVE,F63QULDVES.KYCLNT,F63QULDVES.LINKUE,F63QULDVES.MFSSTK,F63QULDVES.PERSON,F63QULDVES.PORACC,F63QULDVES.PORTFOLIO,F63QULDVES.PRDCLX,F63QULDVES.PRDPRD,F63QULDVES.PRODUC,F63QULDVES.PRODUCT_TYPE,F63QULDVES.RISK_PROFILE_DEFINITION,F63QULDVES.ROTCLI,F63QULDVES.RSKPRF,F63QULDVES.S18CFG,F63QULDVES.SECURITY,F63QULDVES.SECURITY_ADDITIONAL_DETAIL,F63QULDVES.SECURITY_DETAIL_S_SECTION,F63QULDVES.SRVLVL,F63QULDVES.STKCLSFH,F63QULDVES.STOCK_CLASSIFICATION_ASSIGNATION,F63QULDVES.STOCK_CLASSIFICATION_DETAIL,F63QULDVES.UEDTST, offset.processed=true}} ConnectRecord{topic='tracey.servers.jhc.co.uk.F63QULDVES.AKBAL', kafkaPartition=null, key=Struct{AKBCLI=000037I,AKBPRD=ISA,AKBCMP=S,AKBYR=2008,AKBBAL=SUB}, keySchema=Schema{tracey.servers.jhc.co.uk.F63QULDVES.AKBAL.Key:STRUCT}, value=Struct{AKBCLI=000037I,AKBPRD=ISA,AKBCMP=S,AKBYR=2008,AKBBAL=SUB,AKBV1=7200.000000,AKBV2=0.000000,__deleted=false}, valueSchema=Schema{tracey.servers.jhc.co.uk.F63QULDVES.AKBAL.Value:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}
//        Map<String, ?> sourcePartition =  
//        Map<String, ?> sourceOffset = 
//        String topic = "topic",
//        Integer partition,
//        Schema keySchema, Object key, Schema valueSchema, Object value
//        
//        
//        SourceRecord sr = new SourceRecord();
//        .newRecord("com.foo.TOPIC", 1, keySchema, keyObject(), valueSchema, valueObject(), System.currentTimeMillis());
//    }
    
    @Test
    public void testRenameSchema() {
        RenamedSchema lower = lca.renameSchema(keySchema);
        assertTrue(lower.isRenamed);
        assertEquals("com.foo.thingkey", lower.schema.name());
        Field first = lower.schema.fields().get(0);
        assertEquals("key_column1", first.name());
    }
    
    private static Object keyObject() {
        Struct s = new Struct(keySchema);
        s.put("KEY_COLUMN1", "Key-Column1-Value");
        return s;
    }

    private static Object valueObject() {
        Struct s = new Struct(keySchema);
        s.put("COLUMN1", "Column1-Value");
        s.put("COLUMN2", "Column2-Value");
        return s;
    }    
    
    private static Schema keySchema() {
        SchemaBuilder sb = new SchemaBuilder(Type.STRUCT);
        sb.name("com.foo.ThingKey");
        sb.field("KEY_COLUMN1", Schema.STRING_SCHEMA);
        return sb.build();
    }

    private static Schema valueSchema() {
        SchemaBuilder sb = new SchemaBuilder(Type.STRUCT);
        sb.name("com.foo.ThingValue");
        sb.field("COLUMN1", Schema.STRING_SCHEMA);
        sb.field("COLUMN2", Schema.STRING_SCHEMA);
        return sb.build();
    }
    
}
