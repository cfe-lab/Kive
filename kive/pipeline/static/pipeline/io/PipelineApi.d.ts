import { Point } from "../canvas/geometry";

interface RestApiResponse {
    readonly url: string;
    readonly removal_plan: string;
    readonly absolute_url: string;
}

export interface PipelineForApi {
    family?: string;
    revision_name?: string;
    revision_desc?: string;
    revision_parent?: number;
    published?: boolean;
    users_allowed?: string[];
    groups_allowed?: string[];
    family_desc?: string;
    canvas_width?: number;
    canvas_height?: number;
    steps?: StepForApi[];
    inputs?: ApiXputData[];
    outcables?: OutcableForApi[];
}
export interface PipelineFromApi extends RestApiResponse {
    readonly family: string;
    readonly id: number;
    readonly display_name: string;
    readonly revision_name: string;
    readonly revision_desc: string;
    readonly revision_parent: number;
    readonly revision_number: number;
    readonly revision_DateTime: string;
    readonly published: boolean;
    readonly user: string;
    readonly users_allowed: string[];
    readonly groups_allowed: string[];
    readonly steps: StepFromApi[];
    readonly inputs: ApiXputData[];
    readonly outputs: ApiXputData[];
    readonly outcables: OutcableFromApi[];
    readonly step_updates: string;
    readonly view_url: string;
}

export interface ApiXputData extends Point {
    dataset_name: string;
    dataset_idx: number;
    structure: ApiCdtData;
}

interface BaseStep extends Point {
    transformation: number;  // to retrieve Method
    name: string;
    fill_colour: string;
    cables_in: ApiCableData[];
    step_num: number;  // 1-index (pipeline inputs are index 0)
}
export interface StepForApi extends BaseStep {
    transformation_type: string;
    new_code_resource_revision_id: number;
    new_outputs_to_delete_names: string[];
    new_dependency_ids?: number[];
}
interface StepFromApi extends BaseStep {
    transformation_family: number;
    inputs: ApiXputData[];
    outputs: ApiXputData[];
    outputs_to_delete: any[];
}

interface BaseCable {
    source?: number;
    source_dataset_name: string;
    source_step?: number;
    custom_wires: ApiCustomWire[];
}
interface BaseOutcable extends BaseCable {
    output_idx: number;
    output_name: string;
    output_cdt: number;
}
interface OutcableFromApi extends BaseOutcable {
    pk: number;
}
export interface OutcableForApi extends BaseOutcable, Point {
}

export interface ApiCableData extends BaseCable {
    dest?: number;
    dest_dataset_name: string;
    keep_output: boolean;
}

interface ApiCustomWire {
    [key: string]: any;
}
interface ApiCdtData {
    compounddatatype: number;
    min_row: null;
    max_row: null;
}