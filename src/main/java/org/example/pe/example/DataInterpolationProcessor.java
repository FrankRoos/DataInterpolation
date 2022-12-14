/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.example.pe.example;


import org.apache.commons.math3.analysis.UnivariateFunction;
import org.apache.commons.math3.analysis.interpolation.*;
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;
import org.apache.streampipes.commons.exceptions.SpRuntimeException;
import org.apache.streampipes.model.DataProcessorType;
import org.apache.streampipes.model.graph.DataProcessorDescription;
import org.apache.streampipes.model.runtime.Event;
import org.apache.streampipes.model.schema.PropertyScope;
import org.apache.streampipes.sdk.builder.ProcessingElementBuilder;
import org.apache.streampipes.sdk.builder.StreamRequirementsBuilder;
import org.apache.streampipes.sdk.helpers.*;
import org.apache.streampipes.sdk.utils.Assets;
import org.apache.streampipes.vocabulary.SO;
import org.apache.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.apache.streampipes.wrapper.routing.SpOutputCollector;
import org.apache.streampipes.wrapper.standalone.ProcessorParams;
import org.apache.streampipes.wrapper.standalone.StreamPipesDataProcessor;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;


public class DataInterpolationProcessor extends StreamPipesDataProcessor {


    private String input_value;
    private String timestamp_value;
    private String interpolation_operation;
    private static final String CHOSEN_TIMESTAMP = "chosen_timestamp";
    private static final String INTERPOLATION_VALUE = "interpolation_value";
    private static final String INPUT_VALUE = "value";
    private static final String TIMESTAMP_VALUE = "timestamp_value";
    private static final String THRESHOLD = "threshold";
    private static final String INTERPOLATION_OPERATION = "interpolation_operation";

    private Double threshold;
    double[] arrayX = {0.0, 0.0};
    double[] arrayY = {0.0, 0.0};
    double[] array3X = {0.0, 0.0, 0.0};
    double[] array3Y = {0.0, 0.0, 0.0};


    @Override
    public DataProcessorDescription declareModel() {
        return ProcessingElementBuilder.create("org.gft.processors.interpolation")
                .withAssets(Assets.DOCUMENTATION, Assets.ICON)
                .withLocales(Locales.EN)
                .category(DataProcessorType.AGGREGATE)

                .requiredStream(StreamRequirementsBuilder.create()
                        .requiredPropertyWithUnaryMapping(EpRequirements.numberReq(),
                                Labels.withId(INPUT_VALUE), PropertyScope.NONE)
                        .requiredPropertyWithUnaryMapping(EpRequirements.numberReq(),
                                Labels.withId(TIMESTAMP_VALUE), PropertyScope.NONE)
                        .build())
                .requiredSingleValueSelection(Labels.withId(INTERPOLATION_OPERATION), Options.from("Linear",
                        "Spline", "Cubic", "Neville"))

                // "Loess"  temporarily removed because to be reviewed */

                .requiredFloatParameter(Labels.withId(THRESHOLD))
                .outputStrategy(OutputStrategies.append(EpProperties.doubleEp(Labels.withId(CHOSEN_TIMESTAMP), "chosen_timestamp", SO.Number),
                        EpProperties.doubleEp(Labels.withId(INTERPOLATION_VALUE), "interpolation_value", SO.Number)))
                .build();
    }

    @Override
    public void onInvocation(ProcessorParams processorParams,
                             SpOutputCollector out,
                             EventProcessorRuntimeContext ctx) throws SpRuntimeException {

        this.input_value = processorParams.extractor().mappingPropertyValue(INPUT_VALUE);
        this.timestamp_value = processorParams.extractor().mappingPropertyValue(TIMESTAMP_VALUE);
        this.threshold = processorParams.extractor().singleValueParameter(THRESHOLD, Double.class);
        this.interpolation_operation = processorParams.extractor().selectedSingleValue(INTERPOLATION_OPERATION, String.class);

    }

    @Override
    public void onEvent(Event event, SpOutputCollector out) {

        double xi;
        double yi = 0.0;

        //recovery input value
        Double value = event.getFieldBySelector(this.input_value).getAsPrimitive().getAsDouble();

        //recovery timestamp value
        String timestampStr = event.getFieldBySelector(this.timestamp_value).getAsPrimitive().getAsString();

        //convert timestamp to double
        double timestamp = Double.parseDouble(timestampStr);

        //recover type of interpolation

        //two-position array interpolation
        if ((this.interpolation_operation.equals("Linear")) || (this.interpolation_operation.equals("Neville"))) {

            System.out.println("two-position array interpolation ");
            //System.out.println("timestamp: " + timestamp );
            //System.out.println("arrayY[0]: " + arrayY[0] );

            //if we are in the first event it sets the [0] values of the two arrays with the data arriving from SP
            if ((arrayY[0] == 0.0 && arrayX[0] == 0.0)) {

                arrayX[0] = timestamp;
                arrayY[0] = value;

                //if the new timestamp is equal than the timestamp previously or the difference is more low to the threshold,
                //do not perform an interpolation
            } else if ((arrayX[0] == timestamp) || (timestamp - arrayX[0] < this.threshold)) {
                System.out.println("--------- Timestamp Values not accepted ------- ");

                //perform an interpolation
            } else {
                arrayX[1] = timestamp;
                arrayY[1] = value;

                //perform a mathematical median for an array of two timestamp values
                BigDecimal bd = BigDecimal.valueOf((arrayX[0] + arrayX[1]) / 2).setScale(2, RoundingMode.HALF_UP);
                xi = bd.doubleValue();

                System.out.println("arrayX: " + Arrays.toString(arrayX));
                System.out.println("arrayY: " + Arrays.toString(arrayY));

                switch (this.interpolation_operation) {
                    case "Linear":
                        //perform a linear interpolation
                        System.out.println("this.interpolation_operation " + this.interpolation_operation);
                        yi = linearInterpolation(arrayX, arrayY, xi);
                        break;
                    case "Neville":
                        //perform a Loess interpolation
                        System.out.println("this.interpolation_operation " + this.interpolation_operation);
                        yi = nevilleInterpolation(arrayX, arrayY, xi);
                        break;
                }

                //move the second value of the array to the first position
                arrayX[0] = arrayX[1];
                arrayY[0] = arrayY[1];

                //set the values resulting from the interpolation, in the fields of the event output
                event.addField("chosen_timestamp", xi);
                event.addField("interpolation_value", yi);

                out.collect(event);
            }

            //three-position array interpolation
        } else if ((this.interpolation_operation.equals("Loess")) || (this.interpolation_operation.equals("Spline")) || (this.interpolation_operation.equals("Cubic"))) {


            System.out.println("three-position array interpolation ");

            //if we are in the first event it sets the [0] values of the two arrays with the data arriving from SP
            if ((array3Y[0] == 0.0 && array3X[0] == 0.0)) {

                array3X[0] = timestamp;
                array3Y[0] = value;

                //if we are in the second event it sets the [1] values of the two arrays with the data arriving from SP
            } else if ((array3Y[1] == 0.0 && array3X[1] == 0.0)) {

                array3X[1] = timestamp;
                array3Y[1] = value;

                //if the new timestamp is equal than the timestamp previously or the difference is more low to the threshold,
                //do not perform an interpolation
            } else if ((array3X[0] == timestamp) || (array3X[1] == timestamp) || (timestamp - array3X[0] < this.threshold) || (timestamp - array3X[1] < this.threshold)) {
                System.out.println("--------- Timestamp Values not accepted ------- ");

                //perform an interpolation
            } else {
                array3X[2] = timestamp;
                array3Y[2] = value;

                //perform a mathematical median for an array of two timestamp values
                BigDecimal bd = BigDecimal.valueOf((array3X[0] + array3X[1] + array3X[2]) / 3).setScale(2, RoundingMode.HALF_UP);
                xi = bd.doubleValue();

                System.out.println("array3X: " + Arrays.toString(array3X));
                System.out.println("array3Y: " + Arrays.toString(array3Y));

                switch (this.interpolation_operation) {
                    case "Loess":
                        //perform a Loess interpolation
                        System.out.println("this.interpolation_operation " + this.interpolation_operation);
                        yi = loessInterpolation(array3X, array3Y, xi);
                        break;
                    case "Spline":
                        //perform a Spline interpolation
                        System.out.println("this.interpolation_operation " + this.interpolation_operation);
                        yi = splineInterpolation(array3X, array3Y, xi);
                        break;
                    case "Cubic":
                        //perform a Loess interpolation
                        System.out.println("this.interpolation_operation " + this.interpolation_operation);
                        yi = cubicInterpolation(array3X, array3Y, xi);
                        break;
                }

                //move the second value of the array to the first position
                array3X[0] = array3X[1];
                array3Y[0] = array3Y[1];
                //move the third value of the array to the second position
                array3X[1] = array3X[2];
                array3Y[1] = array3Y[2];

                //set the values resulting from the interpolation, in the fields of the event output
                event.addField("chosen_timestamp", xi);
                event.addField("interpolation_value", yi);

                out.collect(event);


            }

        }

    }

    @Override
    public void onDetach() {
    }


    public double linearInterpolation(double[] x, double[] y, double xi) {
        // return linear interpolation of (x,y) on xi
        LinearInterpolator li = new LinearInterpolator();
        PolynomialSplineFunction psf = li.interpolate(x, y);
        return psf.value(xi);
    }

    public double loessInterpolation(double[] x, double[] y, double xi) {
        LoessInterpolator li = new LoessInterpolator();
        PolynomialSplineFunction psf = li.interpolate(x, y);
        return psf.value(xi);
    }

    public double splineInterpolation(double[] x, double[] y, double xi) {
        SplineInterpolator si = new SplineInterpolator();
        PolynomialSplineFunction psf = si.interpolate(x, y);
        return psf.value(xi);
    }

    public double cubicInterpolation(double[] x, double[] y, double xi) {
        UnivariateInterpolator si = new SplineInterpolator();
        UnivariateFunction uf = si.interpolate(x, y);
        return uf.value(xi);
    }

    public double nevilleInterpolation(double[] x, double[] y, double xi) {
        UnivariateInterpolator ni = new NevilleInterpolator();
        UnivariateFunction uf = ni.interpolate(x, y);
        return uf.value(xi);
    }


}